package com.ibm.crail.benchmarks.tests.tpcds

import com.ibm.crail.benchmarks.{ParseOptions, SQLTest}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by atr on 04.09.17.
  */
class TPCDSTest (val options: ParseOptions, spark:SparkSession) extends SQLTest(spark){
  // you set up the temp view
  TPCDSSetup.readAndRegisterTempTables(options, spark)
  // we need 100 queries

  private val result:Array[DataFrame] = new Array[DataFrame](TPCDSQueries.query.size)
  private var time:Array[Long] = new Array[Long](TPCDSQueries.query.size)
  private var i:Int = 0
  for ((k,v) <- TPCDSQueries.query) {
    // key, we ignore. We use the key in the printout
    result(i) = spark.sql(v)
    i+=1
  }

  override def execute(): String = {
    // notice "until"
    for( i <- 0 until result.size) {
      val s = System.nanoTime()
      takeAction(options, result(i))
      time(i) = System.nanoTime() - s
    }
    s"${options.getAction.toString} for TPCDS"
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = s"TPC-DS on " + options.getInputFiles()(0)

  override def printAdditionalInformation():String = {
    val sb = new StringBuilder
    i = 0;
    for ((k,v) <- TPCDSQueries.query){
      sb.append(" query " + k + " took : " + (time(i) / 1000000) + " msec")
      i+=1
    }
    sb.mkString
  }
}
