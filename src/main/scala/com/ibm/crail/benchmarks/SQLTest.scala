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

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

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
        "count result " + result.count() + " items "
      }
      case Save(format: String, fileName: String) => {
        result.write.format(format).mode(SaveMode.Overwrite).save(fileName)
        ("saved " + fileName + " in format " + format)
      }
      case _ => throw new Exception("Illegal action")
    }
  }

  def execute():String

  def explain()

  def plainExplain(): String
}
