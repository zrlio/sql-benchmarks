package com.ibm.crail.benchmarks.tests.tpcds

import com.ibm.crail.benchmarks.ParseOptions
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 04.09.17.
  */
object TPCDSSetup {
  private var items = Array(
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site"
  )

  def readAndRegisterTempTables(options:ParseOptions, spark:SparkSession):Unit = {
    items.foreach( tab => {
      spark.read.format(options.getInputFormat).options(options.getInputFormatOptions).load(options.getInputFiles()(0)+tab).createOrReplaceTempView(tab)
    })
  }
}
