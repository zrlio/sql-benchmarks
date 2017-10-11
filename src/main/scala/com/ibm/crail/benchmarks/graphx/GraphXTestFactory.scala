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
package com.ibm.crail.benchmarks.graphx

import com.ibm.crail.benchmarks.{BaseTest, GraphXOptions}
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 11.10.17.
  */
object GraphXTestFactory {

  def getTestObject(graphxOptions:GraphXOptions, spark:SparkSession):BaseTest = {
    if(graphxOptions.getTestName.compareToIgnoreCase("pageRank") == 0) {
      new PageRankTest(spark, graphxOptions)
    } else {
      throw new Exception(" NYI")
    }
  }
}
