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
package com.ibm.crail.benchmarks.sql

import com.ibm.crail.benchmarks._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * Created by atr on 26.04.17.
  */
abstract class SQLTest(val spark: SparkSession)  extends BaseTest {

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
  private val colName = "renameColumn"
  private var intSuffix = 0

  private def getNextColumnName():String = {
    val s = colName + intSuffix
    intSuffix+=1
    s
  }

  private def isAValidName(str:String):Boolean = {
    toMatch.map(f => if(str.contains(f)) Some(true) else None).count(_.isDefined) == 0
  }

  private def eliminateIllegalColumnNames(df:Dataset[_]):(Dataset[_], Boolean) = {
    val colNames = df.columns
    var finalDS = df
    var reanmed = false
    colNames.foreach( name => {
      if(!isAValidName(name)){
        finalDS = finalDS.withColumnRenamed(name, getNextColumnName())
        reanmed = true
      }
    })
    (finalDS, reanmed)
  }

  private def schemaToString(sch:StructType):String = {
    val strB1 = new StringBuilder
    strB1.append("[ ")
    sch.foreach(f => strB1.append(f.toString() + " "))
    strB1.append(" ]")
    strB1.mkString
  }

  private def removeDuplicateColumnNames(df:Dataset[_]):(Dataset[_], Boolean) = {
    val colNames = df.columns
    val colFrequency = colNames.map( c => {
      (c, colNames.map( c2 => if(c2.compareTo(c) == 0) Option(1) else None).count(_.isDefined))
    })
    val newColNames = colFrequency.map(kv => if(kv._2 == 1) kv._1 else getNextColumnName()).toSeq
    val finalDs = df.toDF(newColNames: _*)
    // if we contain frequency > 1
    val renamed = colFrequency.count(kv => kv._2 > 1) > 0
    (finalDs, renamed)
  }

  private def sanitizeColumnNames(input:Dataset[_]):(Dataset[_], Option[String]) = {
    // first eliminate illegal names
    val step1 = eliminateIllegalColumnNames(input)
    // rename duplicates
    val step2 = removeDuplicateColumnNames(step1._1)
    val finalDS = step2._1
    val sb = new StringBuilder()
    if(step1._2) {
      sb.append("\n\t\t **NOTE:** column renaming happened because the result dataset contains illegal column names.")
    }
    if(step2._2){
      sb.append("\n\t\t **NOTE:** column renaming happened because the result dataset contains duplicate column names.")
    }
    val strx = if(step1._2 || step2._2) {
      sb.append("\n\t\t result dataset columns : " + schemaToString(input.schema) +
        "\n\t\t clean result dataset columns : " + schemaToString(finalDS.schema) + "\n")
      Some(sb.mkString)
    } else {
      None
    }
    (finalDS, strx)
  }

  def takeAction(options: SQLOptions, result: Dataset[_], suffix:String=""):String = {
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
        res._1.write.format(fmt).options(options.getOutputFormatOptions).mode(SaveMode.Overwrite).save(fileName+suffix)
        "saved " + fileName + " in format " + fmt + res._2.getOrElse("")
      }
      case _ => throw new Exception("Illegal action ")
    }
  }

  def takeActionArray(options: SQLOptions, result: Array[Dataset[_]]):String = {
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
}
