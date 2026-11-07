package fpa

import fpa.common.{GoldTransformations, ReferenceValidation}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ReferencePipelineSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]")
      .appName("ReferencePipelineSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def json(lines: String*): DataFrame = {
    val session = spark
    import session.implicits._
    session.read.json(lines.toDS())
  }

  private def errors(df: DataFrame): Set[String] =
    df.select(explode(col("validation_errors"))).collect().map(_.getString(0)).toSet

  private def companies: DataFrame = json(
    """{"company_code":"1000","company_currency":"EUR"}"""
  )

  private def gl: DataFrame = json(
    """{"gl_account":"400000","gl_category":"REVENUE"}"""
  )

  private def costCenters: DataFrame = json(
    """{"company_code":"1000","cost_center":"10000001","profit_center":"P100001"}"""
  )

  private def profitCenters: DataFrame = json(
    """{"company_code":"1000","profit_center":"P100001"}"""
  )

  private def plan(
      version: String = "1",
      description: String = "Budget",
      amount: String = "100.00",
      company: String = "1000",
      currency: String = "EUR",
      costCenter: String = "10000001",
      row: Long = 2L
  ): String =
    s"""{"BUKRS":"$company","GJAHR":"2025","MONAT":"1","VERSION":"$version","VERSION_TXT":"$description","KOSTL":"$costCenter","PRCTR":"P100001","HKONT":"400000","CATEGORY":"REVENUE","WAERS":"$currency","DMBTR":"$amount","source_filename":"plan_versions.csv","source_row_number":$row,"ingested_at":"2026-01-01T00:00:00Z","batch_id":"batch-1"}"""

  test("planning version and decimal conversion errors are preserved together") {
    val checked = ReferenceValidation.validatePlans(
      json(plan(version = "3", description = "Other", amount = "not-a-decimal")),
      companies, gl, costCenters, profitCenters)
    assert(errors(checked).contains("INVALID_PLAN_VERSION"))
    assert(errors(checked).contains("INVALID_PLAN_AMOUNT"))
  }

  test("duplicate planning keys reject every row in the duplicate group") {
    val checked = ReferenceValidation.validatePlans(
      json(plan(row = 2), plan(row = 3)), companies, gl, costCenters, profitCenters)
    assert(ReferenceValidation.invalid(checked).count() == 2)
    assert(errors(checked).contains("DUPLICATE_PLAN_KEY"))
  }

  test("missing company and cost center mappings are explicit") {
    val unknownCompany = json(plan(company = "9999"))
    val unknownCostCenter = json(plan(costCenter = "DOES_NOT_EXIST", row = 3))
    val checked = ReferenceValidation.validatePlans(
      unknownCompany.unionByName(unknownCostCenter), companies, gl, costCenters, profitCenters)
    assert(errors(checked).contains("UNKNOWN_COMPANY_CODE"))
    assert(errors(checked).contains("UNKNOWN_COST_CENTER"))
  }

  test("company currency mismatch is rejected") {
    val checked = ReferenceValidation.validatePlans(
      json(plan(currency = "USD")), companies, gl, costCenters, profitCenters)
    assert(errors(checked).contains("COMPANY_CURRENCY_MISMATCH"))
  }

  test("invalid GL classifications are rejected") {
    val raw = json(
      """{"HKONT":"400000","TXT50":"Revenue","CATEGORY":"UNKNOWN","NORMAL_SIDE":"X","SEASONALITY":"flat","PL_RATIO":"2.0","source_filename":"dim_gl_account.csv","source_row_number":2,"ingested_at":"2026-01-01T00:00:00Z","batch_id":"batch-1"}"""
    )
    val checked = ReferenceValidation.validateGlAccount(raw)
    assert(errors(checked).contains("INVALID_GL_CATEGORY"))
    assert(errors(checked).contains("INVALID_NORMAL_SIDE"))
    assert(errors(checked).contains("PL_RATIO_OUT_OF_RANGE"))
  }

  test("revenue credit sign normalization negates the accounting amount") {
    val result = json("""{"amount":125.50,"normal_side":"H"}""")
      .withColumn("amount", col("amount").cast("decimal(20,2)"))
      .select(ReferenceValidation.normalizeReportingAmount(col("amount"), col("normal_side")).as("reporting"))
      .first().getDecimal(0)
    assert(result.compareTo(new java.math.BigDecimal("-125.50")) == 0)
  }

  test("Budget and Rolling Forecast remain separate during aggregation") {
    val plans = json(
      """{"company_code":"1000","fiscal_year":2025,"period":1,"version":"1","cost_center":"C1","profit_center":"P1","gl_account":"400000","plan_amount":100.00}""",
      """{"company_code":"1000","fiscal_year":2025,"period":1,"version":"2","cost_center":"C1","profit_center":"P1","gl_account":"400000","plan_amount":120.00}"""
    ).withColumn("plan_amount", col("plan_amount").cast("decimal(20,2)"))
    val row = ReferenceValidation.aggregatePlans(plans).first()
    assert(row.getAs[java.math.BigDecimal]("budget_raw").compareTo(new java.math.BigDecimal("100.00")) == 0)
    assert(row.getAs[java.math.BigDecimal]("forecast_raw").compareTo(new java.math.BigDecimal("120.00")) == 0)
  }

  test("full outer Gold join retains actual-only and plan-only rows") {
    val actual = json(
      """{"company_code":"1000","fiscal_year":2025,"period":1,"cost_center":"A","profit_center":"P1","gl_account":"400000","actual_raw":10.00,"posting_count":1}"""
    )
    val plans = json(
      """{"company_code":"1000","fiscal_year":2025,"period":1,"cost_center":"B","profit_center":"P1","gl_account":"400000","budget_raw":20.00,"forecast_raw":25.00}"""
    )
    val result = GoldTransformations.metrics(actual, plans)
    assert(result.count() == 2)
    assert(result.filter(col("has_actual") && !col("has_budget")).count() == 1)
    assert(result.filter(!col("has_actual") && col("has_budget")).count() == 1)
  }

  test("duplicate-key reconciliation reports a critical failure condition") {
    val duplicated = json("""{"key":"A"}""", """{"key":"A"}""")
    assert(GoldTransformations.duplicateKeyCount(duplicated, Seq("key")) == 1)
  }

  test("variance reconciliation detects a corrupted formula") {
    val actual = json(
      """{"company_code":"1000","fiscal_year":2025,"period":1,"cost_center":"A","profit_center":"P1","gl_account":"400000","actual_raw":10.00,"posting_count":1}"""
    )
    val plans = json(
      """{"company_code":"1000","fiscal_year":2025,"period":1,"cost_center":"A","profit_center":"P1","gl_account":"400000","budget_raw":8.00,"forecast_raw":9.00}"""
    )
    val corrupted = GoldTransformations.metrics(actual, plans)
      .withColumn("budget_variance", col("budget_variance") + 1)
    assert(GoldTransformations.varianceMismatchCount(corrupted) == 1)
  }
}
