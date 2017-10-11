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
package com.ibm.crail.benchmarks

import com.ibm.crail.benchmarks.sql.tpcds.{SingleTPCDSTest, TPCDSTest}
import com.ibm.crail.benchmarks.sql.{EquiJoin, SQLTest}
import com.ibm.crail.benchmarks.tests.ReadOnly
import org.apache.spark.sql.SparkSession

object SQLTestFactory {

  def getTestObject(sqlOptions:SQLOptions, spark:SparkSession) : SQLTest = {
    if(sqlOptions.isTestEquiJoin) {
      new EquiJoin(sqlOptions, spark)
    } else if (sqlOptions.isTestQuery) {
      new SingleTPCDSTest(sqlOptions, spark)
    } else if (sqlOptions.isTestTPCDS) {
      new TPCDSTest(sqlOptions, spark)
    } else if (sqlOptions.isTestReadOnly) {
      new ReadOnly(sqlOptions, spark)
    } else {
      throw new Exception("Illegal test name ")
    }
  }
}

//else if (options.isTestPaquetReading) {
//  val item = ParquetTest.process(options.getInputFiles)
//  println(" %%%%%%%%%%%%% " + item._1.size + " items ")
//  new ParquetTest(item, spark)
//} else if (options.isTestSFFReading) {
//  val item = ParquetTest.process(options.getInputFiles)
//  new SFFReadingTest(item, spark)
//}