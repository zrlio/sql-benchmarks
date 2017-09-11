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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * Created by atr on 26.04.17.
  */
abstract class SQLTest(val spark: SparkSession) {

  private val toMatch = Array(
    " ",
    ",",
    ";",
    "{",
    "}",
    "(",
    ")",
    "\n",
    "\t",
    "=")
  private val renameCol = "renameCol"
  private var intSuffix = 0

  private def isAValidName(str:String):Boolean = {
    toMatch.map(f => if(str.contains(f)) Some(true) else None).count(_.isDefined) == 0
  }

  private def sanitizeColumnNames(df:Dataset[_]):(Dataset[_], Boolean) = {
    val colNames = df.columns
    var finalDS = df
    var reanmed = false
    colNames.foreach( name => {
      if(!isAValidName(name)){
        finalDS = finalDS.withColumnRenamed(name, renameCol+intSuffix)
        intSuffix+=1
        reanmed = true
      }
    })
    //System.err.println(" -------------- input ------------- ")
    //System.err.println(df.printSchema())
    //System.err.println(" -------------- sanitized input ------------- ")
    //System.err.println(finalDS.printSchema())
    (finalDS, reanmed)
  }

  def schemaToString(sch:StructType):String = {
    val strB1 = new StringBuilder
    strB1.append("[ ")
    sch.foreach(f => strB1.append(f.toString() + " "))
    strB1.append(" ]")
    strB1.mkString
  }

  def takeAction(options: ParseOptions, result: Dataset[_]):String = {
    val action = options.getAction
    action match {
      case Collect(items: Int) => {
        "collected " + result.limit(items).collect().length + " items"
      }
      case Count() => {
        "count, Dataset.persist().count() " + result.persist().count() + " items "
      }
      //option("compression","none")
      case Save(fileName: String) => {
        val fmt = options.getOutputFormat
        val res = sanitizeColumnNames(result)
        val sanizedResult = res._1
        val renamed = res._2
        sanizedResult.write.format(fmt).options(options.getOutputFormatOptions).mode(SaveMode.Overwrite).save(fileName)
        val resultString = "saved " + fileName + " in format " + fmt
        val note = if(renamed) {
          "\n\t\t **NOTE:** column renaming happened because the final dataset contains illegal column names. See below: " +
          "\n\t\t final dataset columns : " + schemaToString(result.schema) +
          "\n\t\t renamed dataset columns : " + schemaToString(sanizedResult.schema) + "\n"
        } else {
          ""
        }
        resultString + note
      }
      case _ => throw new Exception("Illegal action ")
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

  def printAdditionalInformation():String = "AdditionalInformation: None"
}
