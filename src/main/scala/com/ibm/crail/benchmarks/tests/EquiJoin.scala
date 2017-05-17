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

class EquiJoin(val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {
  private val file1 = options.getInputFiles()(0)
  private val file2 = options.getInputFiles()(1)
  private val f1 = spark.read.parquet(file1)
  private val f2 = spark.read.parquet(file2)
  private val key = options.getJoinKey
  private val result = f1.joinWith(f2, f1(key) === f2(key))

  override def execute(): String = takeAction(options, result)

  override def explain(): Unit = result.explain(true)

  override def plainExplain(): String = "EquiJoin (joinWith) on " + file1 + " and " + file2 + " with key " + key
}
