package com.ibm.crail.benchmarks.tests.tpcds

import com.ibm.crail.benchmarks.{ParseOptions, SQLTest}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by atr on 04.09.17.
  */
class TPCDSTest (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark){
  // you set up the temp view
  TPCDSSetup.readAndRegisterTempTables(options, spark)
  // we need 100 queries
  case class ResultWithQuery(df:DataFrame, queryName:String)

  private val result:Array[ResultWithQuery] = new Array[ResultWithQuery](TPCDSQueries.query.size)
  private var time:Array[Long] = new Array[Long](TPCDSQueries.query.size)
  private var i:Int = 0
  for ((k,v) <- TPCDSQueries.query) {
    result(i) = new ResultWithQuery(spark.sql(v), k)
    i+=1
  }

  override def execute(): String = {
    // notice "until"
    for( i <- 0 until result.length) {
      val s = System.nanoTime()
      takeAction(options, result(i).df)
      time(i) = System.nanoTime() - s
      println((i + 1) + "/" + result.length + " executed query : " + result(i).queryName + " on " + options.getInputFiles()(0) + " took " + time(i)/1000000 + " msec ")
    }
    s"${options.getAction.toString} for TPCDS"
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = s"TPC-DS on " + options.getInputFiles()(0)

  override def printAdditionalInformation():String = {
    val sb = new StringBuilder
    i = 0
    for ((k,v) <- TPCDSQueries.query){
      sb.append(" query " + k + " took : " + (time(i) / 1000000) + " msec\n")
      i+=1
    }
    sb.mkString
  }
}
