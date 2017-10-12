package com.ibm.crail.benchmarks.fio

import com.ibm.crail.benchmarks.{BaseTest, FIOOptions, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 12.10.17.
  */
class HdfsWriteTest (fioOptions:FIOOptions, spark:SparkSession) extends BaseTest  {
  val baseName = "/hdfsfile"
  private val fullPathFileNames = {
    for (i <- 0 until fioOptions.getNumTasks) yield fioOptions.getInputLocations + baseName + i
  }
  private val iotime = spark.sparkContext.longAccumulator("iotime")
  private val setuptime = spark.sparkContext.longAccumulator("setuptime")
  private val requestSize = fioOptions.getRequetSize
  private val times = fioOptions.getSizePerTask / requestSize
  private val rdd = spark.sparkContext.parallelize(fullPathFileNames, fioOptions.getParallelism)

  override def execute(): String = {
    rdd.foreach(fx => {

      val s1 = System.nanoTime()
      val conf = new Configuration()
      val path = new Path(fx)
      val uri = path.toUri
      val fs:FileSystem = FileSystem.get(uri, conf)
      val ostream = fs.create(path)
      val buffer = new Array[Byte](requestSize)

      val s2 = System.nanoTime()
      for ( i <- 0L until times){
        ostream.write(buffer)
      }
      // flushes the client buffer
      ostream.hflush()
      // to the disk
      ostream.hsync()
      val s3 = System.nanoTime()

      ostream.close()
      val s4 = System.nanoTime()

      iotime.add(s3 -s2)
      setuptime.add(s2 -s1)
      setuptime.add(s4 -s3)
    })
    "Wrote " + fullPathFileNames.size + " HDFS files in " + fioOptions.getInputLocations + " directory, each size " + fioOptions.getSizePerTask + " bytes"
  }

  override def explain(): Unit ={}

  override def plainExplain(): String = "Hdfs write test"

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val bw = Utils.twoLongDivToDecimal(8L * fioOptions.getNumTasks * fioOptions.getSizePerTask, timelapsedinNanosec)
    val ioTime = Utils.twoLongDivToDecimal(iotime.value, Utils.MICROSEC)
    val setupTime = Utils.twoLongDivToDecimal(setuptime.value, Utils.MICROSEC)
    val rounds = fioOptions.getNumTasks / fioOptions.getParallelism
    "Bandwidth is           : " + bw + " Gbps \n"+
      "Total, io time         : " + ioTime + " msec | setuptime " + setupTime + " msec | (numTasks: " + fioOptions.getNumTasks + ", parallelism: " + fioOptions.getParallelism + ", rounds: " + rounds + "\n"
//    +"Average, io time/stage : " + Utils.decimalRound(ioTime/fioOptions.getNumTasks.toDouble) +
//      " msec | setuptime " + Utils.decimalRound(setupTime/fioOptions.getNumTasks.toDouble) + " msec\n"+
//    "NOTE: keep in mind that if tasks > #cpus_in_the_cluster then you need to adjust the average time\n"
  }
}