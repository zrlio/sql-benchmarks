/*
 * Crail SQL Benchmarks
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
package com.ibm.crail.benchmarks.tests

import com.ibm.crail.benchmarks.{ParseOptions, SQLTest}
import org.apache.spark.sql.SparkSession

class Q65Rewrite(val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {
  private val location = options.getInputFiles()(0)
  private val suffix=""

  // we first read in the tables
  private val storeSales = spark.read.parquet(location+"/store_sales"+suffix)
  private val dateDim    = spark.read.parquet(location+"/date_dim"+suffix)
  private val store = spark.read.parquet(location+"/store"+suffix)
  private val item = spark.read.parquet(location+"/item"+suffix)

  private val sa1 = storeSales.join(dateDim, storeSales("ss_sold_date_sk") === dateDim("d_date_sk"))
  private val sa2 = sa1.where("d_year == 2001")
  private val sa3 =  sa2.groupBy("ss_store_sk", "ss_item_sk")
  private val sa4 = sa3.sum("ss_sales_price").withColumnRenamed("sum(ss_sales_price)", "revenue")

  private val sc = sa4
  private val sa = sa4

  private val sc1 = sc.join(item, sc("ss_item_sk") === item("i_item_sk"))
  private val sc2 = sc1.join(store, sc("ss_store_sk") === store("s_store_sk"))

  private val sb = sa4.groupBy("ss_store_sk").avg("revenue").withColumnRenamed("avg(revenue)", "ave")

  private val sc3 = sc2.join(sb, sc("ss_store_sk") === sb("ss_store_sk"))

  //val res1 = sc3.where(sc("revenue") <= sb("ave").*(0.1)) // on synthetic data this does not work

  private val res1 = sc3.where(sc("revenue") <= sb("ave"))
  private val result = res1
    .orderBy("s_store_name", "i_item_desc")
    .select("s_store_name", "i_item_desc", "revenue", "i_current_price", "i_wholesale_cost", "i_brand")

  override def execute(): String = takeAction(options, result)

  override def explain(): Unit = result.explain(true)

  override def plainExplain(): String = ("TPC-DS query 65 (re-write) on " + location)
}
