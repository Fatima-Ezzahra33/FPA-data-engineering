package fpa.silver

import fpa.common.{ReferenceSchemas, ReferenceValidation}

import java.time.Instant

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object ReferenceProcessor {

  private val Base = "hdfs://namenode:9000/fpa"

  private case class Quality(total: Long, valid: Long, invalid: Long, duplicates: Long)
  private case class Output(name: String, data: DataFrame, active: String, quarantine: Boolean = false)

  private def parseBronze(spark: SparkSession, root: String, batchId: String, schema: StructType): DataFrame = {
    val bronze = spark.read.parquet(s"$root/snapshot-$batchId")
    val parsed = bronze.withColumn(
      "parsed",
      from_csv(col("raw_line"), schema, Map("mode" -> "PERMISSIVE"))
    )
    parsed.select(
      col("parsed.*"), col("raw_line"), col("raw_header"),
      col("source_filename"), col("source_row_number"),
      col("ingested_at"), col("batch_id")
    )
  }

  private def quality(name: String, validated: DataFrame): Quality = {
    val total = validated.count()
    val invalid = ReferenceValidation.invalid(validated).count()
    val duplicates = validated.filter(col("is_duplicate") === true).count()
    val result = Quality(total, total - invalid, invalid, duplicates)
    println(
      s"QUALITY $name total=${result.total} valid=${result.valid} " +
        s"invalid=${result.invalid} duplicate_rows=${result.duplicates}"
    )
    result
  }

  private def publish(fs: FileSystem, staged: Path, active: Path, archiveRoot: Path): Unit = {
    fs.mkdirs(active.getParent)
    var archived: Option[Path] = None
    if (fs.exists(active)) {
      fs.mkdirs(archiveRoot)
      val archive = new Path(archiveRoot, active.toString.replace("hdfs://namenode:9000/fpa/", "").replace("/", "__"))
      require(fs.rename(active, archive), s"Could not archive active dataset $active to $archive")
      archived = Some(archive)
    }
    if (!fs.rename(staged, active)) {
      archived.foreach(old => fs.rename(old, active))
      throw new IllegalStateException(s"Could not publish staged dataset $staged to $active")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FPA-Reference-Silver-Processor").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      val planRoot = s"$Base/bronze/plans"
      val requestedBatch = args.headOption
      val batchId = requestedBatch.getOrElse {
        spark.read.parquet(s"$planRoot/snapshot-*")
          .orderBy(col("ingested_at").desc)
          .select("batch_id").first().getString(0)
      }
      println(s"=== REFERENCE SILVER PROCESSING batch_id=$batchId ===")

      val companyRaw = parseBronze(spark, s"$Base/bronze/dimensions/company_code", batchId, ReferenceSchemas.companyCode)
      val glRaw = parseBronze(spark, s"$Base/bronze/dimensions/gl_account", batchId, ReferenceSchemas.glAccount)
      val pcRaw = parseBronze(spark, s"$Base/bronze/dimensions/profit_center", batchId, ReferenceSchemas.profitCenter)
      val groupRaw = parseBronze(spark, s"$Base/bronze/dimensions/cost_center_group", batchId, ReferenceSchemas.costCenterGroup)
      val ccRaw = parseBronze(spark, s"$Base/bronze/dimensions/cost_center", batchId, ReferenceSchemas.costCenter)
      val planRaw = parseBronze(spark, planRoot, batchId, ReferenceSchemas.plan)

      val companyChecked = ReferenceValidation.validateCompany(companyRaw).cache()
      val company = ReferenceValidation.valid(companyChecked).drop("is_duplicate").cache()
      quality("company_code", companyChecked)

      val glChecked = ReferenceValidation.validateGlAccount(glRaw).cache()
      val gl = ReferenceValidation.valid(glChecked).drop("is_duplicate", "pl_ratio_raw").cache()
      quality("gl_account", glChecked)

      val pcChecked = ReferenceValidation.validateProfitCenter(pcRaw, company).cache()
      val pc = ReferenceValidation.valid(pcChecked).drop("is_duplicate").cache()
      quality("profit_center", pcChecked)

      val groupChecked = ReferenceValidation.validateCostCenterGroup(groupRaw, company).cache()
      val groups = ReferenceValidation.valid(groupChecked).drop("is_duplicate").cache()
      quality("cost_center_group", groupChecked)

      val ccChecked = ReferenceValidation.validateCostCenter(ccRaw, company, pc, groups).cache()
      val cc = ReferenceValidation.valid(ccChecked).drop("is_duplicate").cache()
      quality("cost_center", ccChecked)

      val planChecked = ReferenceValidation.validatePlans(planRaw, company, gl, cc, pc).cache()
      val plans = ReferenceValidation.valid(planChecked)
        .drop("is_duplicate", "fiscal_year_raw", "period_raw", "plan_amount_raw")
        .cache()
      quality("plans", planChecked)

      val runId = s"$batchId-${Instant.now().toEpochMilli}"
      val stagingRoot = s"$Base/_staging/reference-silver/$runId"
      val archiveRoot = new Path(s"$Base/_archive/reference-silver/$runId")
      val outputs = Seq(
        Output("company_code", company, s"$Base/silver/dimensions/company_code"),
        Output("gl_account", gl, s"$Base/silver/dimensions/gl_account"),
        Output("profit_center", pc, s"$Base/silver/dimensions/profit_center"),
        Output("cost_center_group", groups, s"$Base/silver/dimensions/cost_center_group"),
        Output("cost_center", cc, s"$Base/silver/dimensions/cost_center"),
        Output("plans", plans, s"$Base/silver/plans"),
        Output("company_code_quarantine", ReferenceValidation.invalid(companyChecked), s"$Base/quarantine/dimensions/company_code", true),
        Output("gl_account_quarantine", ReferenceValidation.invalid(glChecked), s"$Base/quarantine/dimensions/gl_account", true),
        Output("profit_center_quarantine", ReferenceValidation.invalid(pcChecked), s"$Base/quarantine/dimensions/profit_center", true),
        Output("cost_center_group_quarantine", ReferenceValidation.invalid(groupChecked), s"$Base/quarantine/dimensions/cost_center_group", true),
        Output("cost_center_quarantine", ReferenceValidation.invalid(ccChecked), s"$Base/quarantine/dimensions/cost_center", true),
        Output("plans_quarantine", ReferenceValidation.invalid(planChecked), s"$Base/quarantine/plans", true)
      )

      outputs.foreach { output =>
        val partitions = if (output.name == "plans") 8 else 1
        output.data.coalesce(partitions)
          .write.mode("errorifexists").parquet(s"$stagingRoot/${output.name}")
      }

      val fs = new Path(Base).getFileSystem(spark.sparkContext.hadoopConfiguration)
      outputs.foreach { output =>
        publish(fs, new Path(s"$stagingRoot/${output.name}"), new Path(output.active), archiveRoot)
      }

      println(s"REFERENCE SILVER PROCESSING COMPLETED batch_id=$batchId")
      println(s"Validated plans: $Base/silver/plans")
      println(s"Validated dimensions: $Base/silver/dimensions")
      println(s"Rejected records: $Base/quarantine/plans and $Base/quarantine/dimensions")
    } finally {
      spark.stop()
    }
  }
}
