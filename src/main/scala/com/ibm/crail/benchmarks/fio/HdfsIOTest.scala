package com.ibm.crail.benchmarks.fio

import com.ibm.crail.benchmarks.{BaseTest, FIOOptions}
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 11.10.17.
  */
class HdfsIOTest(fioOptions:FIOOptions, spark:SparkSession) extends BaseTest {

  override def execute(): String = ???

  override def explain(): Unit = ???

  override def plainExplain(): String = ???
}
