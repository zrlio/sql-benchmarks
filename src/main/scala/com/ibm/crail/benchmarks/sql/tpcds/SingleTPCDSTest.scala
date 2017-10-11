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
import com.ibm.crail.benchmarks.sql.SQLTest
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 04.09.17.
  */
class SingleTPCDSTest(val sqlOptions: SQLOptions, spark:SparkSession) extends SQLTest(spark) {
  // you set up the temp view
  TPCDSSetup.readAndRegisterTempTables(sqlOptions, spark)
  private val query = TPCDSQueries.query(sqlOptions.getTPCDSQuery+".sql")
  private val result = spark.sql(query)

  override def execute(): String = takeAction(sqlOptions, result)

  override def explain(): Unit = result.explain(true)

  override def plainExplain(): String = s"TPC-DS query ${sqlOptions.getTPCDSQuery} on " + sqlOptions.getInputFiles()(0)

  override def printAdditionalInformation(): String = s"SQL query ${sqlOptions.getTPCDSQuery}: ${query}"

}
