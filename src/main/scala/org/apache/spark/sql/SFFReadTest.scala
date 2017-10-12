package org.apache.spark.sql

import com.ibm.crail.benchmarks.fio.FIOUtils
import com.ibm.crail.benchmarks.{BaseTest, FIOOptions, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.catalyst.InternalRow
/**
  * Created by atr on 12.10.17.
  */
class SFFReadTest(fioOptions:FIOOptions, spark:SparkSession) extends BaseTest {
  private val filesEnumerated = FIOUtils.enumerateWithSize(fioOptions.getInputLocations)
  println(filesEnumerated)
  var totalBytesExpected = 0L
  filesEnumerated.foreach(fx => {
    totalBytesExpected = totalBytesExpected + fx._2
  })

  private val iotime = spark.sparkContext.longAccumulator("iotime")
  private val setuptime = spark.sparkContext.longAccumulator("setuptime")
  private val totalRows = spark.sparkContext.longAccumulator("totalRows")
  private val rdd = spark.sparkContext.parallelize(filesEnumerated, fioOptions.getParallelism)
  private val inputDir = fioOptions.getInputLocations

  override def execute(): String = {
    /**
      * A part (i.e. "block") of a single file that should be read, along with partition column values
      * that need to be prepended to each row.
      *
      * @param partitionValues value of partition columns to be prepended to each row.
      * @param filePath path of the file to read
      * @param start the beginning offset (in bytes) of the block.
      * @param length number of bytes to read.
      * @param locations locality information (list of nodes that have the data).
      */

    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      val sff = new SimpleFileFormat
      val schema = FIOUtils.inferSFFSchema(inputDir, spark)
      val conf = new Configuration()
      val readerFunc = sff.buildReader(spark,
        schema.get,
        null,
        schema.get,
        Seq[Filter](),
        Map[String, String](),
        conf)
      //spark.sparkContext.hadoopConfiguration - this is NULL
      val filePart = PartitionedFile(InternalRow.empty, fx._1, 0, fx._2)
      val readerItr = readerFunc(filePart)

      val s2 = System.nanoTime()
      var rowsx = 0L
      while(readerItr.hasNext){
        readerItr.next().asInstanceOf[UnsafeRow]
        rowsx+=1
      }
      val s3 = System.nanoTime()

      iotime.add(s3 -s2)
      setuptime.add(s2 -s1)
      totalRows.add(rowsx)
    })
    "SFF read " + filesEnumerated.size + " HDFS files in " + fioOptions.getInputLocations + " directory, total rows " + totalRows.value
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "SFF reading test on " + inputDir

  override def printAdditionalInformation(timelapsedinNanosec:Long): String = {
    val bw = Utils.twoLongDivToDecimal(totalBytesExpected, timelapsedinNanosec)
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
