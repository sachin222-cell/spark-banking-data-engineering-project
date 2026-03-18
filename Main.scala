package com.freelancer

import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Balanceaggregation")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val balanceDF = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/input/balanceAccount.csv")
    val customerF = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/input/customerAccount.csv")

    val balanceRenameDF = balanceDF.withColumnRenamed("business_date", "balance_business_date")
    val customerRenameDF = customerF.withColumnRenamed("business_date", "customer_business_date")

    balanceRenameDF.show()
    customerRenameDF.show()

    val aggregatedDF = balanceRenameDF.groupBy(
        col("account_no"),
        col("pdp_product_description"),
        col("fiscal_year"),
        col("fiscal_period"),
        col("balance_business_date")
      )
      .agg(
        sum(when(col("balance_income_type") === "DEBIT_INTEREST", col("amount")).otherwise(lit(0))).alias("debit_interest_amt"),
        sum(when(col("balance_income_type") === "FUNDING", col("amount")).otherwise(lit(0))).alias("funding_amt"),
        sum(when(col("balance_income_type") === "tot_income_amt", col("amount")).otherwise(lit(0))).alias("tot_income_amt"),
        sum(when(col("balance_income_type") === "nii_amt", col("amount")).otherwise(lit(0))).alias("nii_amt"),
        sum(when(col("balance_income_type") === "OTHER", col("amount")).otherwise(lit(0))).alias("other_amt"),
        sum(when(col("balance_income_type") === "FEES_AND_COMMISSION", col("amount")).otherwise(lit(0))).alias("fees_commision_amt"),
        sum(when(col("balance_income_type") === "ftp_amt", col("amount")).otherwise(lit(0))).alias("ftp_amt"),
        sum(when(col("balance_income_type") === "swaps_amt", col("amount")).otherwise(lit(0))).alias("swaps_amt")

      )
    aggregatedDF.show()

    val derivedDF = aggregatedDF.withColumn("nii_amt",
        col("debit_interest_amt") +
          col("funding_amt")
      )
      .withColumn("tot_income_amt",
        col("nii_amt") +
          col("other_amt") +
          col("fees_commision_amt")
      )

    val joinedDF = derivedDF.join(customerRenameDF,
      derivedDF("account_no") === customerRenameDF("customer_id"),
      "left"
    )
    joinedDF.show()

    val finalDF = joinedDF.select(
      col("stg_id"),
      col("account_no"),
      col("customer_id"),
      col("pdp_product_description"),
      col("fiscal_year"),
      col("fiscal_period"),
      col("debit_interest_amt"),
      col("funding_amt"),
      col("ftp_amt"),
      col("swaps_amt"),
      col("other_amt"),
      col("nii_amt"),
      col("tot_income_amt"),
      col("fees_commision_amt")
    )
    finalDF.show()

    finalDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("C:/output/output_cust_bal")

  }
}

