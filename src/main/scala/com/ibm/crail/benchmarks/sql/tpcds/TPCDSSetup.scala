/*
 * Spark Benchmarks
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *
 * Copyright (C) 2017, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.benchmarks.sql.tpcds

import com.ibm.crail.benchmarks.SQLOptions
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

  def readAndRegisterTempTables(sqlOptions:SQLOptions, spark:SparkSession):Unit = {
    items.foreach( tab => {
      spark.read.format(sqlOptions.getInputFormat).options(sqlOptions.getInputFormatOptions).load(sqlOptions.getInputFiles()(0)+"/"+tab).createOrReplaceTempView(tab)
    })
  }
}
