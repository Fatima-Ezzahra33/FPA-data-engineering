
package fpa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaConsumer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FPA-Accounting-Streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Read accounting events from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:19092")
      .option("subscribe", "fpa.actuals.v1")
      .option("startingOffsets", "earliest")
      .load()

    // 2. Parse the JSON payload
    val parsedDF = kafkaDF
      .select(
        col("value").cast("string").as("raw_json"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").as("kafka_timestamp")
      )
      .withColumn(
        "event",
        from_json(col("raw_json"), EventSchema.accountingEvent)
      )

    // 3. Extract and type accounting fields
    val accountingDF = parsedDF.select(
        col("event.event_id").as("event_id"),
        col("event.event_type").as("event_type"),
        col("event.posting.RBUKRS").as("company_code"),
        col("event.posting.GJAHR").as("fiscal_year"),
        col("event.posting.BELNR").as("document_number"),
        col("event.posting.DOCLN").as("document_line"),
        col("event.posting.RACCT").as("gl_account"),
        col("event.posting.RCNTR").as("cost_center"),
        col("event.posting.BUDAT").as("posting_date_raw"),
        col("event.posting.HSL").as("amount_raw"),
        col("topic"),
        col("partition"),
        col("offset"),
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

    // 4. Validate required fields
    val validatedDF = accountingDF.withColumn(
      "is_valid",
      col("event_id").isNotNull &&
        (length(trim(col("event_id"))) > 0) &&
        (col("event_type") === "FI_POSTING") &&
        col("company_code").isNotNull &&
        (length(trim(col("company_code"))) > 0) &&
        col("document_number").isNotNull &&
        (length(trim(col("document_number"))) > 0) &&
        col("document_line").isNotNull &&
        (length(trim(col("document_line"))) > 0) &&
        col("gl_account").isNotNull &&
        (length(trim(col("gl_account"))) > 0) &&
        col("amount").isNotNull &&
        col("posting_date").isNotNull
    )

    // 5. Display structured accounting records
    val query = validatedDF
      .select(
        "event_id",
        "company_code",
        "gl_account",
        "cost_center",
        "amount",
        "posting_date",
        "is_valid"
      )
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()

    try {
      query.awaitTermination()
    } finally {
      query.stop()
      spark.stop()
    }
  }
}
