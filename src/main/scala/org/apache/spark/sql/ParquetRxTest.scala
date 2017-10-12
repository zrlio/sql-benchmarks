package org.apache.spark.sql

import com.ibm.crail.benchmarks.sql.SQLTest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.GeneratedIteratorIntWithPayload
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.collection.JavaConverters._

/**
  * Created by atr on 22.09.17.
  */
object ParquetTest {

  val SFFMetadataExtension:String = "-mdata"

  def isSFFMetaFile(path:String):Boolean = {
    path.substring(path.length - SFFMetadataExtension.length, path.length).compareTo(SFFMetadataExtension) == 0
  }

  def ok(path:Path):Boolean = {
    val fname = path.getName
    fname(0) != '_' && fname(0) != '.' && !isSFFMetaFile(fname)
  }

  def process(fileNames:Array[String]):(List[String], Long) = {
    fileNames.map( p => {
      println(" processing " + p)
      _process(p)
    }).reduce((i1, i2) => (i1._1 ++ i2._1, i1._2+i2._2))
  }

  def _process(fileName:String):(List[String], Long) = {
    val path = new Path(fileName)
    val conf = new Configuration()
    val fileSystem = path.getFileSystem(conf)
    // we get the file system
    val fileStatus:Array[FileStatus]  = fileSystem.listStatus(path)
    val files = fileStatus.map(_.getPath).filter(ok).toList
    var totalBytes = 0L
    files.map( fx => fileSystem.getFileStatus(fx)).foreach(fx => totalBytes+=fx.getLen)
    (files.map(fx => fx.toString), totalBytes)
  }
}

class ParquetTest (val item:(List[String], Long), spark:SparkSession) extends SQLTest(spark) {
  // we have all the files now
  val inx = spark.sparkContext.parallelize(item._1)

  val totalRows = spark.sparkContext.longAccumulator("totalRows")
  override def execute(): String = {
    inx.foreach { p =>
      /* from there on we use the generated code */
      val vectorizedReader = new VectorizedParquetRecordReader
      vectorizedReader.initialize(p, List("intKey", "payload").asJava)
      vectorizedReader.enableReturningBatches()

      val recordIterator = new RecordReaderIterator(vectorizedReader).asInstanceOf[Iterator[InternalRow]]
      val objArr = new Array[Object](2)
      // these are dummy SQL metrics we can remove them eventually
      objArr(0) = new SQLMetric("atr1", 0L)
      objArr(1) = new SQLMetric("atr1", 0L)
      val generatedIterator = new GeneratedIteratorIntWithPayload(objArr)
      generatedIterator.init(0, Array(recordIterator))

      while(generatedIterator.hasNext){
        generatedIterator.next().asInstanceOf[UnsafeRow]
        totalRows.add(1L)
      }
    }
    "Consumed auto gen iterator "
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = " read " + item._2 + " bytes in " + totalRows.value + " rows"
}
