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
package org.apache.spark.sql

import com.ibm.crail.benchmarks.fio.FIOUtils
import com.ibm.crail.benchmarks.{BaseTest, FIOOptions, Utils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.GeneratedIteratorIntWithPayload
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.execution.metric.SQLMetric

/**
  * Created by atr on 12.10.17.
  */
class ParquetReadTest(fioOptions:FIOOptions, spark:SparkSession) extends BaseTest {

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


  override def execute(): String = {
    rdd.foreach(fx =>{
      val s1 = System.nanoTime()
      /* from there on we use the generated code */
      import scala.collection.JavaConverters._
      val vectorizedReader = new VectorizedParquetRecordReader
      vectorizedReader.initialize(fx._1, List("intKey", "payload").asJava)
      vectorizedReader.enableReturningBatches()

      val recordIterator = new RecordReaderIterator(vectorizedReader).asInstanceOf[Iterator[InternalRow]]
      val objArr = new Array[Object](2)
      // these are dummy SQL metrics we can remove them eventually
      objArr(0) = new SQLMetric("atr1", 0L)
      objArr(1) = new SQLMetric("atr1", 0L)
      val generatedIterator = new GeneratedIteratorIntWithPayload(objArr)
      generatedIterator.init(0, Array(recordIterator))
      val s2 = System.nanoTime()
      var rowsx = 0L

      while(generatedIterator.hasNext){
        generatedIterator.next().asInstanceOf[UnsafeRow]
        rowsx+=1
      }
      val s3 = System.nanoTime()

      iotime.add(s3 -s2)
      setuptime.add(s2 -s1)
      totalRows.add(rowsx)
    })
    "Parquet<int,payload> read " + filesEnumerated.size + " HDFS files in " + fioOptions.getInputLocations + " directory, total rows " + totalRows.value
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = "Parquet<int,payload> reading test"

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
