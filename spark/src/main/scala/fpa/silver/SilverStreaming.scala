
package fpa.silver

import fpa.common.EventSchema

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.StreamingQuery

object SilverStreaming {

  private val hdfs = "hdfs://namenode:9000/fpa"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FPA-Silver-Streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Explicit schema for Bronze Parquet files
    val bronzeSchema = StructType(Seq(
      StructField("kafka_key", StringType),
      StructField("raw_json", StringType),
      StructField("topic", StringType),
      StructField("partition", IntegerType),
      StructField("offset", LongType),
      StructField("kafka_timestamp", TimestampType)
    ))

    // 1. Continuously discover new Bronze files
    val bronze = spark.readStream
      .schema(bronzeSchema)
      .option("maxFilesPerTrigger", 10)
      .parquet(s"$hdfs/bronze/actuals")

    // 2. Parse the nested JSON
    val parsed = bronze.withColumn(
      "event",
      from_json(
        col("raw_json"),
        EventSchema.accountingEvent
      )
    )

    // 3. Extract and type accounting fields
    val accounting = parsed.select(
        col("event.event_id").as("event_id"),
        col("event.event_type").as("event_type"),
        col("event.schema_version").as("schema_version"),

        col("event.posting.RLDNR").as("ledger"),
        col("event.posting.RBUKRS").as("company_code"),
        col("event.posting.GJAHR").as("fiscal_year"),
        col("event.posting.POPER").as("posting_period"),
        col("event.posting.BELNR").as("document_number"),
        col("event.posting.DOCLN").as("document_line"),
        col("event.posting.RACCT").as("gl_account"),
        col("event.posting.RCNTR").as("cost_center"),
        col("event.posting.PRCTR").as("profit_center"),
        col("event.posting.RWCUR").as("currency"),
        col("event.posting.DRCRK").as("debit_credit"),
        col("event.posting.STBLG").as("reversal_document"),

        col("event.posting.HSL").as("amount_raw"),
        col("event.posting.BUDAT").as("posting_date_raw"),

        col("topic"),
        col("partition").as("kafka_partition"),
        col("offset").as("kafka_offset"),
        col("kafka_timestamp"),
        col("raw_json")
      )
      .withColumn(
        "amount",
        expr("try_cast(amount_raw AS DECIMAL(20,2))")
      )
      .withColumn(
        "posting_date",
        expr("try_cast(posting_date_raw AS DATE)")
      )

    // 4. Validate each event
    val validated = accounting.withColumn(
      "validation_error",
      when(
        col("event_id").isNull ||
          length(trim(col("event_id"))) === 0,
        "MISSING_EVENT_ID"
      )
        .when(
          col("event_type").isNull ||
            col("event_type") =!= "FI_POSTING",
          "INVALID_EVENT_TYPE"
        )
        .when(
          col("schema_version").isNull ||
            col("schema_version") =!= 1,
          "UNSUPPORTED_SCHEMA_VERSION"
        )
        .when(
          col("company_code").isNull ||
            length(trim(col("company_code"))) === 0,
          "MISSING_COMPANY_CODE"
        )
        .when(
          col("fiscal_year").isNull ||
            length(trim(col("fiscal_year"))) === 0,
          "MISSING_FISCAL_YEAR"
        )
        .when(
          col("document_number").isNull ||
            length(trim(col("document_number"))) === 0,
          "MISSING_DOCUMENT_NUMBER"
        )
        .when(
          col("document_line").isNull ||
            length(trim(col("document_line"))) === 0,
          "MISSING_DOCUMENT_LINE"
        )
        .when(
          col("gl_account").isNull ||
            length(trim(col("gl_account"))) === 0,
          "MISSING_GL_ACCOUNT"
        )
        .when(
          col("currency").isNull ||
            length(trim(col("currency"))) === 0,
          "MISSING_CURRENCY"
        )
        .when(col("amount").isNull, "INVALID_AMOUNT")
        .when(
          col("posting_date").isNull,
          "INVALID_POSTING_DATE"
        )
        .otherwise(lit(null).cast("string"))
    )

    // 5. Deduplicate valid events across micro-batches
    val silver = validated
      .filter(col("validation_error").isNull)
      .dropDuplicates("event_id")
      .drop(
        "validation_error",
        "amount_raw",
        "posting_date_raw",
        "raw_json"
      )

    // 6. Keep invalid events for investigation
    val quarantine = validated
      .filter(col("validation_error").isNotNull)

    // 7. Continuously write trusted records
    val silverQuery = silver.writeStream
      .format("parquet")
      .outputMode("append")
      .option(
        "path",
        s"$hdfs/silver/actuals_live"
      )
      .option(
        "checkpointLocation",
        s"$hdfs/checkpoints/silver_live"
      )
      .partitionBy("fiscal_year", "company_code")
      .start()

    // 8. Continuously write invalid events
    val quarantineQuery = quarantine.writeStream
      .format("parquet")
      .outputMode("append")
      .option(
        "path",
        s"$hdfs/quarantine/actuals_live"
      )
      .option(
        "checkpointLocation",
        s"$hdfs/checkpoints/quarantine_live"
      )
      .start()

    println("SILVER STREAMING STARTED")
    println("Bronze -> validation -> deduplication -> Silver")
    println("Invalid events -> quarantine")

    try {
      spark.streams.awaitAnyTermination()
    } finally {
      silverQuery.stop()
      quarantineQuery.stop()
      spark.stop()
    }
  }
}
