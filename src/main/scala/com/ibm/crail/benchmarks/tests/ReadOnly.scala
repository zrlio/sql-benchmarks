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
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql._

/**
  * Created by atr on 05.05.17.
  */
class ReadOnly(val options: ParseOptions, spark:SparkSession) extends SQLTest(spark) {
  private val fmt = options.getInputFormat
  private val inputfiles = options.getInputFiles()
  //println("Number of input files are : " + inputfiles.length + " with format " + fmt)

  private var readDataSetArr:Array[Dataset[Row]] = new Array[Dataset[Row]](inputfiles.length)
  var i = 0
  // we first read all of them
  inputfiles.foreach(in => {
    readDataSetArr(i) = spark.read.format(fmt).options(options.getInputFormatOptions).load(in)
    i+=1
  })

  // we then make a union of them
  var finalDataset:Dataset[Row] = readDataSetArr(0)
  for(i <- 1 until readDataSetArr.length){
    finalDataset = readDataSetArr(i).union(finalDataset)
  }

  // we do cache here, because now any action will trigger the whole data set reading
  // even the count().
  override def execute(): String = {
    takeAction(options, finalDataset)
  }

// TODO: this needs to be investigated, the performance difference
//  override def execute(): String = {
//    takeActionArray(options, readDataSetArr)
//  }

  override def explain(): Unit = readDataSetArr(0).explain(true)

  override def plainExplain(): String = {
    var str:String = ""
    inputfiles.foreach(f=> str+=f+",")
    "ReadOnly (with .cache()) test on " + inputfiles.length + " files as: " + str
  }
}
