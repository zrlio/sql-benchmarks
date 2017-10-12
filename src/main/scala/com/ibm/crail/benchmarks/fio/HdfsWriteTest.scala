package com.ibm.crail.benchmarks.fio

import java.io.IOException

import com.ibm.crail.benchmarks.{BaseTest, FIOOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 12.10.17.
  */
class HdfsWriteTest (fioOptions:FIOOptions, spark:SparkSession) extends BaseTest  {


  val baseName = "/hdfsfile"
  private val fullPathFileNames = {
    for (i <- 0 until fioOptions.getParallel) yield (fioOptions.getInputLocations + baseName + i)
  }
  private val rdd = spark.sparkContext.parallelize(fullPathFileNames, fullPathFileNames.size)
  private val iotime = spark.sparkContext.longAccumulator("iotime")
  private val setuptime = spark.sparkContext.longAccumulator("setuptime")
  private val requestSize = fioOptions.getRequetSize
  private val times = fioOptions.getSizePerTask / requestSize

  println(" ***** numer of files are: " +  fullPathFileNames.size)

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
    "Done | total io time: " + iotime.value/1000 + " usec, setuptime " + setuptime.value/1000 + " usec "
  }

  override def explain(): Unit = "Done | total io time: " + iotime.value/1000 + " usec, setuptime " + setuptime.value/1000 + " usec "

  override def plainExplain(): String = "Done | total io time: " + iotime.value/1000 + " usec, setuptime " + setuptime.value/1000 + " usec "
}
