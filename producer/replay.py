"""Replay SAP Universal Journal CSV lines to Kafka as compressed hourly ERP extracts.

Three responsibilities:
  1. SourceReader: validate/read source journal lines without changing SAP values.
  2. ReplayScheduler: assign simulated hourly availability, gaps, and late arrivals.
  3. KafkaPublisher: serialize/publish/acknowledge Kafka records.
"""

import argparse
import csv
import json
import logging
import random
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

LOG = logging.getLogger("fpa.replay")
DEFAULT_CSV = Path(__file__).resolve().parent.parent / "data" / "raw" / "acdoca_universal_journal.csv"
REQUIRED = {"RLDNR", "RBUKRS", "GJAHR", "BELNR", "DOCLN", "BUDAT", "RACCT", "HSL", "RWCUR"}
DOCUMENT_FIELDS = ("RLDNR", "RBUKRS", "GJAHR", "BELNR")
# One journal line = one Kafka record. Documents are NOT grouped into whole batches.


def document_key(row):
    return "|".join(row[name] for name in DOCUMENT_FIELDS)


def event_id(row):
    return document_key(row) + "|" + row["DOCLN"]


class SourceReader:
    def __init__(self, path):
        self.path = Path(path)

    def read(self, limit=0, order="posting-date"):
        rows, seen = [], set()
        with self.path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ValueError("CSV has no header")
            missing = REQUIRED - set(reader.fieldnames)
            if missing:
                raise ValueError("Missing CSV columns: " + ", ".join(sorted(missing)))
            for line_no, row in enumerate(reader, start=2):
                try:
                    if any(not row.get(col) for col in REQUIRED):
                        raise ValueError("Required field missing or empty")
                    amount = Decimal(row["HSL"])
                    if not amount.is_finite():
                        raise ValueError("HSL is not a finite number")
                    date.fromisoformat(row["BUDAT"][:10])
                    identity = event_id(row)
                    if identity in seen:
                        raise ValueError("Duplicate source journal line: " + identity)
                    seen.add(identity)
                except (InvalidOperation, ValueError, TypeError, KeyError) as exc:
                    raise ValueError(f"Invalid CSV line {line_no}: {exc}") from exc
                rows.append(row)
                if limit and len(rows) >= limit:
                    break
        if order == "posting-date":
            # Stable sort by accounting date. This is NOT true source-arrival order.
            rows.sort(key=lambda r: (r["BUDAT"], r["RBUKRS"], r["GJAHR"], r["BELNR"], r["DOCLN"]))
        return rows


class ReplayScheduler:
    def __init__(self, *, profile, min_per_hour, max_per_hour, empty_probability,
                 late_probability, late_min_hours, late_max_hours, seed, simulated_start):
        self.profile = [int(v) for v in profile.split(",")] if profile else None
        self.min_per_hour = min_per_hour
        self.max_per_hour = max_per_hour
        self.empty_probability = empty_probability
        self.late_probability = late_probability
        self.late_min_hours = late_min_hours
        self.late_max_hours = late_max_hours
        self.rng = random.Random(seed)
        self.simulated_start = simulated_start

    def schedule(self, rows):
        """Return [(hour_index, [(source_row, is_delayed), ...])].

        The profile repeats if specified. Random workloads include empty hours.
        A zero-only profile is rejected to prevent a nonterminating schedule.
        """
        if self.profile is not None and not any(v > 0 for v in self.profile):
            raise ValueError("At least one hourly profile slot must be positive")
        slots = defaultdict(list)
        index, hour = 0, 0
        while index < len(rows):
            if self.profile is not None:
                capacity = self.profile[hour % len(self.profile)]
            else:
                capacity = (0 if self.rng.random() < self.empty_probability
                            else self.rng.randint(self.min_per_hour, self.max_per_hour))
            selected = rows[index:index + capacity]
            index += len(selected)
            for row in selected:
                is_late = self.rng.random() < self.late_probability
                extra = self.rng.randint(self.late_min_hours, self.late_max_hours) if is_late else 0
                slots[hour + extra].append((row, is_late))
            hour += 1
        # Include all empty hours between first and last delivery, plus the initial hour.
        last_hour = max(slots, default=-1)
        return [(h, slots[h]) for h in range(last_hour + 1)]


class DeliveryTracker:
    def __init__(self):
        self.delivered = 0
        self.failed = 0

    def callback(self, error, message):
        if error:
            self.failed += 1
            LOG.error("Kafka delivery failed: %s", error)
        else:
            self.delivered += 1
            LOG.debug("Delivered partition=%s offset=%s", message.partition(), message.offset())


class KafkaPublisher:
    def __init__(self, broker, topic):
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError("Install the Kafka client: pip install confluent-kafka") from exc
        self.client = Producer({
            "bootstrap.servers": broker, "acks": "all", "enable.idempotence": True,
            "compression.type": "snappy", "linger.ms": 20,
            "delivery.timeout.ms": 120000, "client.id": "fpa-erp-hourly-replay",
        })
        self.topic = topic
        self.tracker = DeliveryTracker()
        self.submitted = 0

    def send(self, key, payload):
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                self.client.produce(self.topic, key=key.encode("utf-8"), value=data,
                                    callback=self.tracker.callback)
                self.submitted += 1
                break
            except BufferError:
                self.client.poll(1)
        self.client.poll(0)
        while len(self.client) >= 1000:
            self.client.poll(0.5)

    def finish(self):
        pending = self.client.flush(130)
        LOG.info("Kafka: submitted=%d delivered=%d failed=%d pending=%d",
                 self.submitted, self.tracker.delivered, self.tracker.failed, pending)
        if pending or self.tracker.failed or self.tracker.delivered != self.submitted:
            raise RuntimeError("Incomplete Kafka delivery; check broker and producer logs")


def replay(rows_by_hour, args, publisher=None):
    started = time.monotonic()
    totals = Counter()
    for hour, entries in rows_by_hour:
        deadline = started + hour * args.interval_seconds
        if publisher is not None:
            wait = deadline - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        available_at = (args.simulated_start + timedelta(hours=hour)).isoformat()
        batch_id = f"{args.run_id}-hour-{hour:05d}"
        for row, is_delayed in entries:
            payload = {
                "event_id": event_id(row), "event_type": "FI_POSTING", "schema_version": 1,
                "source": "SAP_SYNTHETIC", "replayed_at": datetime.now(timezone.utc).isoformat(),
                "simulation": {"run_id": args.run_id, "batch_id": batch_id,
                               "simulated_available_at": available_at,
                               "late_arrival_injected": is_delayed},
                "posting": row,
            }
            if publisher is not None:
                publisher.send(document_key(row), payload)
            totals["lines"] += 1
            totals["late"] += int(is_delayed)
        totals["hours"] += 1
        totals["empty"] += int(not entries)
        LOG.info("Hour %04d | %s | lines=%d late=%d%s", hour, available_at,
                 len(entries), sum(late for _, late in entries), " EMPTY" if not entries else "")
    if publisher is not None:
        publisher.finish()
    LOG.info("Summary: hours=%d empty_hours=%d journal_lines=%d injected_late=%d",
             totals["hours"], totals["empty"], totals["lines"], totals["late"])
    return totals


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Compressed-hourly SAP accounting replay into Kafka")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--broker", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="fpa.actuals.v1")
    parser.add_argument("--limit", type=int, default=100, help="Journal lines to replay; 0 = all")
    parser.add_argument("--order", choices=("posting-date", "csv"), default="posting-date")
    parser.add_argument("--hourly-profile", default=None,
                        help="Comma-separated journal-line counts, e.g. 10,25,0,40; repeats")
    parser.add_argument("--min-per-hour", type=int, default=5)
    parser.add_argument("--max-per-hour", type=int, default=30)
    parser.add_argument("--empty-probability", type=float, default=0.15)
    parser.add_argument("--late-probability", type=float, default=0.05)
    parser.add_argument("--late-min-hours", type=int, default=1)
    parser.add_argument("--late-max-hours", type=int, default=3)
    parser.add_argument("--interval-seconds", type=float, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--simulated-start", default="2026-01-01T08:00:00+00:00")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Plan batches without Kafka/network/sleep")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    if args.limit < 0 or args.min_per_hour < 1 or args.max_per_hour < args.min_per_hour:
        parser.error("Invalid line limit or hourly capacity")
    if not (0 <= args.empty_probability < 1) or not (0 <= args.late_probability <= 1):
        parser.error("Probabilities must be within allowed ranges")
    if args.late_min_hours < 1 or args.late_max_hours < args.late_min_hours or args.interval_seconds < 0:
        parser.error("Invalid delay or interval")
    if args.hourly_profile is not None:
        try:
            profile = [int(x.strip()) for x in args.hourly_profile.split(",")]
            if not profile or any(x < 0 for x in profile) or not any(profile):
                raise ValueError()
        except ValueError:
            parser.error("--hourly-profile requires nonnegative integers, at least one positive")
    try:
        args.simulated_start = datetime.fromisoformat(args.simulated_start)
        if args.simulated_start.tzinfo is None:
            raise ValueError("Timezone offset required")
    except ValueError as exc:
        parser.error(f"Invalid --simulated-start: {exc}")
    if not args.csv.is_file():
        parser.error(f"CSV missing: {args.csv}")
    args.run_id = args.run_id or datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S%fZ")
    return args


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    rows = SourceReader(args.csv).read(limit=args.limit, order=args.order)
    scheduler = ReplayScheduler(profile=args.hourly_profile, min_per_hour=args.min_per_hour,
                                max_per_hour=args.max_per_hour, empty_probability=args.empty_probability,
                                late_probability=args.late_probability, late_min_hours=args.late_min_hours,
                                late_max_hours=args.late_max_hours, seed=args.seed,
                                simulated_start=args.simulated_start)
    schedule = scheduler.schedule(rows)
    LOG.info("Loaded %d lines; scheduled %d simulated hours", len(rows), len(schedule))
    replay(schedule, args, None if args.dry_run else KafkaPublisher(args.broker, args.topic))


if __name__ == "__main__":
    main()
