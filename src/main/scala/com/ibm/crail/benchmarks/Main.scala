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

package com.ibm.crail.benchmarks

import com.ibm.crail.benchmarks.fio.FIOTestFactory
import com.ibm.crail.benchmarks.graphx.GraphXTestFactory
import org.apache.spark.sql.SparkSession

object Main {

  def foo(x : Array[String]) = x.foldLeft(" ")((a,b) => a + b)

  def getTestOptions(subsystem:String):TestOptions = {
    if(subsystem.compareToIgnoreCase("SQL") == 0){
      new SQLOptions()
    } else if(subsystem.compareToIgnoreCase("Graphx") == 0) {
      new GraphXOptions()
    } else if (subsystem.compareToIgnoreCase("FIO") == 0) {
      new FIOOptions()
    } else {
      throw new Exception("Illegal subsystem (-s) : " + subsystem)
    }
  }

  def getSubsystemTest(testOption:TestOptions, spark:SparkSession):BaseTest = {
    testOption match {
      case gx:GraphXOptions => GraphXTestFactory.getTestObject(gx, spark)
      case sx:SQLOptions => SQLTestFactory.getTestObject(sx, spark)
      case fx:FIOOptions => FIOTestFactory.getTestObject(fx, spark)
      case _ => throw new Exception(" ??? ")
    }
  }

  def main(args : Array[String]) {
    println("concat arguments to the program = " + foo(args))
    val sb:StringBuilder = new StringBuilder
    val mainOptions = new MainOptions
    val subsystemArgs = mainOptions.parseMainOptions(args)
    val subsystemOptions = getTestOptions(mainOptions.getSubsystem)
    subsystemOptions.parse(subsystemArgs)
    val spark = SparkSession.builder.appName("Swiss Spark benchmarks").getOrCreate

    /* now we have everything setup */
    if(subsystemOptions.withWarmup()){
      /* here we do the trick that we execute the whole test before */
      subsystemOptions.setWarmupConfig()
      val warmUpTest = getSubsystemTest(subsystemOptions, spark)
      val s = System.nanoTime()
      val warmupResult = warmUpTest.execute()
      val e = System.nanoTime()
      sb.append("------------- WarmUp ----------------------------------" + "\n")
      sb.append("NOTICE: understand that spark does caching (data and metadata) for input files.\n")
      sb.append("        hence for sensible output use different files between -i & -w.\n")
      sb.append("WarmUp Test           : " + warmUpTest.plainExplain() + "\n")
      sb.append("WarmUp Execution time : " + (e - s)/1000000 + " msec" + "\n")
      sb.append("WarmUp Result         : " +  warmupResult + "\n")
      sb.append("-------------------------------------------------" + "\n")
      // restore
      subsystemOptions.restoreInputConfig()
    }

    val test = getSubsystemTest(subsystemOptions, spark)
    val s = System.nanoTime()
    val result = test.execute()
    val e = System.nanoTime()
    sb.append("-------------------------------------------------" + "\n")
    sb.append("Test           : " + test.plainExplain() + "\n")
    sb.append("Execution time : " + (e - s)/1000000 + " msec" + "\n")
    sb.append("Result         : " +  result + "\n")
    sb.append("---------------- Additional Info ------------------\n")
    sb.append(test.printAdditionalInformation())
    sb.append("-------------------------------------------------\n")
    println(sb.mkString)

    spark.stop()
  }
}
