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

import com.ibm.crail.benchmarks.GraphXOptions
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 11.10.17.
  */
class PageRankTest(spark:SparkSession, options: GraphXOptions) extends GraphXTest {
  /* we got the input file, we then load it */
  private val start = System.nanoTime()
  private val graph = GraphLoader.edgeListFile(spark.sparkContext, options.getGraphFile).cache()
  private val end = System.nanoTime()

  override def execute(): String = {
    /* we don't have to take any action from the datasets */
    org.apache.spark.graphx.lib.PageRank.run(this.graph, options.getPageRankIterations)
    "Ran PageRank " + options.getPageRankIterations + " iterations on " + options.getGraphFile
  }

  override def explain(): Unit = println(plainExplain())

  override def plainExplain(): String = "PageRank " + options.getPageRankIterations + " iterations on " + options.getGraphFile

  override def printAdditionalInformation(timelapsedinNanosec:Long):String = {
    val sb = new StringBuilder()
    sb.append("Graph load time: " + (end - start)/1000000 + " msec\n")
    sb.mkString
  }
}