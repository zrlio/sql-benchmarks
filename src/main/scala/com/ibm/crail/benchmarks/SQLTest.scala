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

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/**
  * Created by atr on 26.04.17.
  */
abstract class SQLTest(val spark: SparkSession) {

  def takeAction(options: ParseOptions, result: Dataset[_]):String = {
    val action = options.getAction
    action match {
      case Collect(items: Int) => {
        "collected " + result.limit(items).collect().length + " items"
      }
      case Count() => {
        "count result (persist) " + result.persist().count() + " items "
      }
      //option("compression","none")
      case Save(fileName: String) => {
        val fmt = options.getOutputFormat
        result.write.format(fmt).options(options.getOutputFormatOptions).mode(SaveMode.Overwrite).save(fileName)
        ("saved " + fileName + " in format " + fmt)
      }
      case _ => throw new Exception("Illegal action")
    }
  }

  def takeActionArray(options: ParseOptions, result: Array[Dataset[_]]):String = {
    val action = options.getAction
    action match {
      case Collect(items: Int) => {
        /* we need to iterator over the array and count number of items to be collected */
        var target:Int = items
        var i = 0
        while( target > 0 && i < result.length) {
          var soFar = result(i).limit(target).collect().length
          target -= soFar
          i += 1
        }
        /* at this point we have exhausted all counts - just print */
        "collected " + (items - target) + " items in " + i + " datasets"
      }

      case Count() => {
        var count = 0L
        result.foreach(ds => count+=ds.count())
        "count result " + count + " items in " + result.length + " datasets"
      }

      case Save(fileName: String) => {
        val fmt = options.getOutputFormat
        result.foreach(ds =>
          ds.write.format(fmt).options(options.getOutputFormatOptions).mode(SaveMode.Overwrite).save(fileName))
        "saved (appended) " + fileName + " in format " + fmt + " for " + result.length + " datasets "
      }
      case _ => throw new Exception("Illegal action")
    }
  }

  def execute():String

  def explain()

  def plainExplain(): String
}
