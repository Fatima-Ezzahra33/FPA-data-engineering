# FP&A Data Engineering Pipeline

Enterprise-style Budget vs Actual data engineering project that simulates SAP Universal Journal transactions, ingests them through Kafka, and processes them with Apache Spark into Bronze, Silver, and Gold datasets on HDFS.

## Technology stack

- Python producer for deterministic SAP accounting replay
- Apache Kafka 4.2.2 for event ingestion
- Apache Spark 4.2.0 with Scala 2.13 for streaming and batch processing
- Apache HDFS for distributed persistence and streaming checkpoints
- Docker Compose with one Spark master, three Spark workers, Kafka, one HDFS NameNode, and one HDFS DataNode

Apache Hive, Apache Airflow, and Power BI are planned integrations; they are not implemented in this repository yet.

## Repository structure

```text
fpa-engineering/
|-- data/raw/                       Source accounting, planning, and dimension CSVs
|-- producer/                       Python SAP journal replay producer
|-- spark/
|   |-- build.sbt                   Scala build definition
|   |-- project/                    sbt project metadata
|   |-- dist/                       Local deployment JAR location
|   `-- src/
|       |-- main/scala/fpa/
|       |   |-- bronze/             Kafka-to-HDFS raw ingestion
|       |   |-- silver/             Streaming and batch validation/normalization
|       |   |-- gold/               Budget vs Actual analytics
|       |   `-- common/             Shared schemas, validation and Gold transformations
|       `-- test/scala/             In-memory Spark validation/reconciliation tests
|-- docker-compose.yml              Kafka, Spark, and HDFS infrastructure
`-- requirements.txt                Python dependencies
```

## Data flows

### Accounting streaming pipeline

```text
Python producer
  -> Kafka topic fpa.actuals.v1
  -> fpa.bronze.KafkaConsumer
  -> HDFS /fpa/bronze/actuals
  -> fpa.silver.SilverStreaming
  -> HDFS /fpa/silver/actuals_live
       and /fpa/quarantine/actuals_live
```

`fpa.silver.SilverProcessor` is the available batch Bronze-to-Silver processor. It writes `/fpa/silver/actuals` and `/fpa/quarantine/actuals`.

### Gold analytics

```text
data/raw planning and dimension CSVs
  -> fpa.bronze.ReferenceIngestion
  -> immutable HDFS Bronze snapshots
  -> fpa.silver.ReferenceProcessor
  -> validated Silver plans and dimensions
       + rejected rows with validation error arrays in quarantine
  -> fpa.gold.GoldProcessor
       + /fpa/silver/actuals_live
  -> reconciled /fpa/gold/budget_vs_actual
```

`fpa.bronze.ReferenceIngestion` validates the six required filenames and exact headers on the driver, preserves each original CSV row as a raw string with its header, filename, source row number, timestamp and content-derived batch ID, and writes immutable snapshots. An identical six-file snapshot is skipped safely.

`fpa.silver.ReferenceProcessor` reads only Bronze snapshots. It validates company and G/L masters first, then profit centers and cost-center groups, then cost centers, and finally plans. Identifiers remain strings. Monetary values use `DECIMAL(20,2)`. Duplicate planning keys are rejected rather than aggregated.

Silver and quarantine publication stages every dataset before replacing its active path. Previous active datasets are moved under `/fpa/_archive`; Gold additionally requires every Silver reference dataset to carry the same batch ID, so a mixed or partial publication cannot be consumed.

`fpa.gold.GoldProcessor` reads only Silver actuals, plans and dimensions. It preserves the existing analytical grain and formulas, reports exclusion/reconciliation metrics, blocks publication on critical failures, validates a staged Parquet snapshot, and atomically swaps the Gold directory while archiving the prior snapshot.

### Reference paths

- Bronze plans: `/fpa/bronze/plans/snapshot-<batch-id>`
- Bronze dimensions: `/fpa/bronze/dimensions/<dataset>/snapshot-<batch-id>`
- Silver plans: `/fpa/silver/plans`
- Silver dimensions: `/fpa/silver/dimensions/<dataset>`
- Plan quarantine: `/fpa/quarantine/plans`
- Dimension quarantine: `/fpa/quarantine/dimensions/<dataset>`
- Gold: `/fpa/gold/budget_vs_actual`

## Build

From the repository root:

```powershell
cd spark
sbt clean test package
Copy-Item target/out/jvm/scala-2.13.18/fpa-spark/fpa-spark_2.13-0.1.0-SNAPSHOT.jar dist/fpa-spark-reference.jar -Force
cd ..
```

The separate `fpa-spark-reference.jar` name avoids replacing the JAR already loaded by active Bronze and Silver streaming drivers. Replace the streaming deployment JAR only during a controlled streaming restart.

## Run the producer

Install dependencies in a virtual environment, then replay records. A dry run validates the CSV and schedule without contacting Kafka:

```powershell
python -m venv .fpa-venv
.\.fpa-venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m producer.replay --dry-run
python -m producer.replay
```

The producer defaults remain `127.0.0.1:9092` and topic `fpa.actuals.v1`.

## Submit Spark applications

The Compose `tools` profile supplies short-lived Spark submit containers. The following commands use the new fully qualified class names:

```powershell
docker compose --profile tools run -d --name fpa-bronze-stream bronze-driver --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --class fpa.bronze.KafkaConsumer --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/fpa/dist/fpa-spark.jar

docker compose --profile tools run -d --name fpa-silver-stream silver-driver --master spark://spark-master:7077 --class fpa.silver.SilverStreaming /opt/fpa/dist/fpa-spark.jar

docker compose --profile tools run --rm spark-submit --master spark://spark-master:7077 --class fpa.silver.SilverProcessor /opt/fpa/dist/fpa-spark.jar

docker compose --profile tools run --rm spark-submit --master spark://spark-master:7077 --class fpa.bronze.ReferenceIngestion /opt/fpa/dist/fpa-spark-reference.jar

docker compose --profile tools run --rm spark-submit --master spark://spark-master:7077 --class fpa.silver.ReferenceProcessor /opt/fpa/dist/fpa-spark-reference.jar

docker compose --profile tools run --rm spark-submit --master spark://spark-master:7077 --class fpa.gold.GoldProcessor /opt/fpa/dist/fpa-spark-reference.jar
```

Follow the detached streaming drivers with `docker logs -f fpa-bronze-stream` and `docker logs -f fpa-silver-stream`. The named containers are intentionally not launched with `--rm`, so startup failures remain available for inspection; remove a stopped driver container explicitly before reusing its name.

Run reference processing in the order shown above: Bronze ingestion, Silver validation, review quality output/quarantine, then Gold. `ReferenceIngestion` uses the read-only Compose mount `/opt/fpa/data/raw`; workers never access Windows-local paths because the driver reads and distributes the small reference files.

To reprocess a specific Bronze batch rather than the newest batch, append its batch ID after the JAR in the `ReferenceProcessor` command.

## Safe streaming deployment and checkpoints

The package rename requires new driver processes to load the rebuilt JAR. It does not require resetting Kafka, deleting HDFS data, removing Docker volumes, or clearing checkpoints.

1. Build and copy the JAR using the commands above.
2. Identify the two existing streaming driver containers and inspect their commands before stopping anything:

   ```powershell
   docker ps --filter ancestor=apache/spark:4.2.0 --format "table {{.ID}}\t{{.Names}}\t{{.Command}}"
   docker inspect --format "{{json .Config.Cmd}}" <bronze-container-id>
   docker inspect --format "{{json .Config.Cmd}}" <silver-container-id>
   ```

3. After confirming the IDs, gracefully stop only those two driver containers. Do not stop Spark workers, Kafka, or HDFS, and do not run `docker compose down -v`:

   ```powershell
   docker stop --time 30 <bronze-container-id>
   docker stop --time 30 <silver-container-id>
   ```

4. Confirm the old applications have disappeared from the Spark UI before submitting replacements, so two writers never share a checkpoint.
5. Submit `fpa.bronze.KafkaConsumer` and `fpa.silver.SilverStreaming` with the commands above.
6. Keep these existing checkpoint locations unchanged:
   - `/fpa/checkpoints/bronze_actuals`
   - `/fpa/checkpoints/silver_live`
   - `/fpa/checkpoints/quarantine_live`

Spark will reuse the checkpoint metadata and continue from the recorded Kafka offsets and file-stream state. The code retains the same source, sink, and checkpoint paths.

## Verification

- Spark master UI: `http://localhost:8080`
- Spark worker UIs: `http://localhost:8081`, `http://localhost:8082`, and `http://localhost:8083`
- HDFS NameNode UI: `http://localhost:9870`
- Bronze: `/fpa/bronze/actuals`
- Bronze reference snapshots: `/fpa/bronze/plans` and `/fpa/bronze/dimensions`
- Live Silver: `/fpa/silver/actuals_live`
- Silver references: `/fpa/silver/plans` and `/fpa/silver/dimensions`
- Live quarantine: `/fpa/quarantine/actuals_live`
- Reference quarantine: `/fpa/quarantine/plans` and `/fpa/quarantine/dimensions`
- Gold: `/fpa/gold/budget_vs_actual`

Example non-destructive HDFS checks:

```powershell
docker compose exec namenode hdfs dfs -ls -R /fpa/bronze
docker compose exec namenode hdfs dfs -ls -R /fpa/silver
docker compose exec namenode hdfs dfs -ls -R /fpa/quarantine
docker compose exec namenode hdfs dfs -ls -R /fpa/gold
docker compose exec namenode hdfs dfs -ls -R /fpa/checkpoints
```

The Silver job prints `total`, `valid`, `invalid`, and `duplicate_rows` for every dataset. Gold prints duplicate-key checks, relationship resolution, actual exclusion counts, signed-actual reconciliation, Budget and Forecast totals, enrichment cardinality, variance consistency, and staged-output checks. Any failed critical check raises an actionable exception before the active Gold path is replaced.

### Validation assumptions

- Planning versions are `1` (Budget) and `2` (Rolling Forecast), and periods are 1–12.
- G/L categories are `REVENUE`, `COGS`, `PERSONNEL`, `OPEX`, `DEPRECIATION`, or `BALANCE_SHEET`; normal sides are `S` or `H`.
- `PL_RATIO` must be decimal in the inclusive range 0–1; seasonality must be populated but is not restricted to a fabricated domain.
- Company countries and currencies use two- and three-letter uppercase codes respectively.
- Duplicate dimension or planning business keys invalidate all rows in the duplicate group.
- Rejected source rows remain in quarantine; valid rows from the same internally consistent batch can be published.
- No currency conversion is performed.

## Current limitations

- Historical accounting ingestion is only as complete as the source CSV data replayed to Kafka.
- Reference ingestion is snapshot-based batch processing rather than streaming CDC.
- Publication across multiple independent Silver paths is not one filesystem transaction; batch IDs make Gold fail closed if an interrupted publication leaves mixed snapshots.
- Hive catalog, Airflow orchestration, and Power BI reporting assets are future work.
