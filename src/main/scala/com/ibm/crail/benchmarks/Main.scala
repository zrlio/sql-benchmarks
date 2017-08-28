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

import org.apache.spark.sql.SparkSession

object Main {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
    val sb:StringBuilder = new StringBuilder
    val options = new ParseOptions()
    options.parse(args)

    val spark = SparkSession.builder.appName("Spark SQL benchmarks").getOrCreate

    if(options.getDoWarmup) {
      /* here we do the trick that we execute the whole test before */
      val warmupOptions = options
      val saveOriginalInputFIle = options.getInputFiles
      warmupOptions.setInputFiles(options.getWarmupInputFiles)
      /* now we execute the test with warmUp options with the input
      files set to the warm up files.
      We can use the output file
       */
      val warningsWarmUp:StringBuilder = new StringBuilder
      val warmUpTest:SQLTest = SQLTestFactory.newTestInstance(warmupOptions, spark, warningsWarmUp)
      val s = System.nanoTime()
      val warmupResult = warmUpTest.execute()
      val e = System.nanoTime()
      sb.append("------------- WarmUp ----------------------------------" + "\n")
      sb.append("NOTICE: understand that spark does caching (data and metadata) for input files.\n")
      sb.append("        hence for sensible output use different files between -i & -w.\n")
      sb.append("WarmUp Test           : " + warmUpTest.plainExplain() + "\n")
      sb.append("WarmUp Action         : " + warmupOptions.getAction.toString + "\n")
      sb.append("WarmUp Execution time : " + (e - s)/1000000 + " msec" + "\n")
      sb.append("WarmUp Result         : " +  warmupResult + "\n")
      if(options.getVerbose){
        sb.append("---------------- warmup explain ----------------------" + "\n")
        sb.append(warmUpTest.explain())
      }
      sb.append("-------------------------------------------------" + "\n")
      // restore
      options.setInputFiles(saveOriginalInputFIle)
    }

    val warningsTest:StringBuilder = new StringBuilder
    val test:SQLTest = SQLTestFactory.newTestInstance(options, spark, warningsTest)
    val s = System.nanoTime()
    val result = test.execute()
    val e = System.nanoTime()
    sb.append("-------------------------------------------------" + "\n")
    sb.append("Test           : " + test.plainExplain() + "\n")
    sb.append("Action         : " + options.getAction.toString + "\n")
    sb.append("Execution time : " + (e - s)/1000000 + " msec" + "\n")
    sb.append("Result         : " +  result + "\n")
    if(options.getVerbose){
      sb.append("---------------- explain ------------------------\n")
      sb.append(test.explain())
    }
    sb.append("-------------------------------------------------\n")
    println(sb.mkString)
    spark.stop()
  }
}
