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

/**
  * Created by atr on 18.05.17.
  */
class Q65String(val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {
  private val location = options.getInputFiles()(0)
  private val suffix=".parquet"
  private val storeSales = spark.read.parquet(location+"/store_sales"+suffix).createOrReplaceTempView("store_sales")
  private val dateDim    = spark.read.parquet(location+"/date_dim"+suffix).createOrReplaceTempView("date_dim")
  private val store = spark.read.parquet(location+"/store"+suffix).createOrReplaceTempView("store")
  private val item = spark.read.parquet(location+"/item"+suffix).createOrReplaceTempView("item")
  //FIXME: format me properly - copied from rst scala scripts for the TPC-DS experiments
  private val query = "select  s_store_name, i_item_desc, sc.revenue, i_current_price, i_wholesale_cost, i_brand from  " +
    "(select    ss_store_sk,    ss_item_sk,    sum(ss_sales_price) as revenue  from    store_sales    join date_dim " +
    "on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)  where  date_dim.d_year = 2001  group by   ss_store_sk,   " +
    "ss_item_sk  ) sc  join item on (sc.ss_item_sk = item.i_item_sk)  join store on " +
    "(sc.ss_store_sk = store.s_store_sk)  join (select ss_store_sk, avg(revenue) as ave from    " +
    "(select      ss_store_sk,      ss_item_sk,      sum(ss_sales_price) as revenue " +
    "from store_sales join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)    " +
    "where date_dim.d_year = 2001  group by ss_store_sk,      ss_item_sk    ) sa  group by    ss_store_sk  ) " +
    "sb on (sc.ss_store_sk = sb.ss_store_sk) where   sc.revenue <= 0.1 * sb.ave order by  s_store_name, i_item_desc"

  private val result = spark.sql(query)

  override def execute(): String = takeAction(options, result)

  override def explain(): Unit = result.explain(true)

  override def plainExplain(): String = ("TPC-DS query 65 (string) on " + location)
}
