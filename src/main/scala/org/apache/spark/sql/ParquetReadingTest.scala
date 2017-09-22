package org.apache.spark.sql

/**
  * Created by atr on 22.09.17.
  */

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.format.converter.ParquetMetadataConverter.range
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileReader.readFooter
import org.apache.parquet.hadoop.metadata.ParquetMetadata

import scala.collection.JavaConverters._
import scala.util.Try
import org.apache.spark.{Accumulator, SparkConf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.util.{AccumulatorV2, Utils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.AtrGeneratedIterator
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.ColumnarBatch

/**
  * Benchmark to measure parquet read performance.
  * To run this:
  *  spark-submit --class <this class> --jars <spark sql test jar>
  */


object ParquetReadingTest {
  //
  //  val conf = new SparkConf()
  //  conf.set("spark.sql.parquet.compression.codec", "uncompressed")
  //
  //  val spark = SparkSession.builder
  //    .master("local[1]")
  //    .appName("test-sql-context")
  //    .config(conf)
  //    .getOrCreate()
  //
  val resultBuilder = new StringBuilder()
  var totalInputHdfsBytes = 0L
  //
  //  // Set default configs. Individual cases will change them if necessary.
  //  spark.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
  //  spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
  //
  //  def withTempPath(f: File => Unit): Unit = {
  //    val path = Utils.createTempDir()
  //    path.delete()
  //    try f(path) finally Utils.deleteRecursively(path)
  //  }
  //
  //  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
  //    val (keys, values) = pairs.unzip
  //    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
  //    (keys, values).zipped.foreach(spark.conf.set)
  //    try f finally {
  //      keys.zip(currentValues).foreach {
  //        case (key, Some(value)) => spark.conf.set(key, value)
  //        case (key, None) => spark.conf.unset(key)
  //      }
  //    }
  //  }

  def ok(path:Path):Boolean = {
    val fname = path.getName()
    fname(0) != '_' && fname(0) != '.'
  }

  def readMyParquetFile(args:Array[String]): Unit = {

    val path = new Path(args(0))
    val conf = new Configuration()
    val fileSystem = path.getFileSystem(conf)

    val fileStatus:Array[FileStatus]  = fileSystem.listStatus(path)
    val files = fileStatus.map(_.getPath).filter(ok).toList

    val mode = if(args.length == 2) args(1).toInt else 3 // run unsafe
    if(mode == 0) {
      // Driving the parquet reader in batch mode directly.
      val start = System.nanoTime()
      var rows = 0L
      files.foreach { p =>
        val fileLength: Long = fileSystem.getFileStatus(p).getLen
        val footer: ParquetMetadata = readFooter(conf, p, range(0, fileLength))
        val fileSchema = footer.getFileMetaData.getSchema
        val count = fileSchema.getFieldCount
        val schemaFields = scala.collection.mutable.ListBuffer.empty[String]
        for (a <- 0 until count) {
          schemaFields += fileSchema.getFieldName(a)
        }
        val reader = new VectorizedParquetRecordReader
        try {
          reader.initialize(p.toString, schemaFields.asJava)
          val batch = reader.resultBatch()
          val col0 = batch.column(0)
          val col1 = batch.column(1)
          while (reader.nextBatch()) {
            val numRows = batch.numRows()
            var i = 0
            while (i < numRows) {
              if (!col0.isNullAt(i)) {
                val id = col0.getInt(i)
              }
              if (!col1.isNullAt(i)) {
                val buf = col1.getBinary(i)
              }
              i += 1
              rows += 1
            }
          }
        } finally {
          reader.close()
        }
      }
      var diff = System.nanoTime() - start

      System.out.println(" counted rows in the batch mode : " + rows + " time: " + diff / 1000 + " us, or " + diff / rows + " ns/Row")
    } else if (mode == 1) {
      // Decoding in vectorized but having the reader return rows.
      val start = System.nanoTime()
      var rows = 0L
      files.foreach { p =>
        val fileLength: Long = fileSystem.getFileStatus(p).getLen
        val footer: ParquetMetadata = readFooter(conf, p, range(0, fileLength))
        val fileSchema = footer.getFileMetaData.getSchema
        val count = fileSchema.getFieldCount
        val schemaFields = scala.collection.mutable.ListBuffer.empty[String]
        for (a <- 0 until count) {
          schemaFields += fileSchema.getFieldName(a)
        }

        val reader = new VectorizedParquetRecordReader
        try {
          reader.initialize(p.toString, schemaFields.asJava)
          val batch = reader.resultBatch()
          while (reader.nextBatch()) {
            val it = batch.rowIterator()
            while (it.hasNext) {
              val record = it.next()
              if (!record.isNullAt(0)) {
                val id = record.getInt(0)
              }
              if (!record.isNullAt(1)) {
                val buf = record.getBinary(1)
              }
              rows += 1
            }
          }
        } finally {
          reader.close()
        }
      }
      val diff = System.nanoTime() - start
      System.out.println(" counted rows in the row mode : " + rows + " time: " + diff / 1000 + " us, or " + diff / rows + " ns/Row")
    } else {
      // Decoding in vectorized but having the reader return UnsafeRow
      val start = System.nanoTime()
      var rows = 0L
      var maxSize = 0L
      files.foreach { p =>
        val fileLength: Long = fileSystem.getFileStatus(p).getLen
        val footer: ParquetMetadata = readFooter(conf, p, range(0, fileLength))
        val fileSchema = footer.getFileMetaData.getSchema
        val count = fileSchema.getFieldCount
        val schemaFields = scala.collection.mutable.ListBuffer.empty[String]
        for (a <- 0 until count) {
          schemaFields += fileSchema.getFieldName(a)
        }
        //TODO: we can move this outside
        val unsafeRow = new UnsafeRow(count)
        val bufferHolder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(unsafeRow, 32)
        val unsafeRowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(bufferHolder, count)

        val reader = new VectorizedParquetRecordReader
        try {
          reader.initialize(p.toString, schemaFields.asJava)
          val batch = reader.resultBatch()

          val col0 = batch.column(0)
          val col1 = batch.column(1)

          while (reader.nextBatch()) {
            val numRows = batch.numRows()
            var i = 0
            while (i < numRows) {
              bufferHolder.reset()
              unsafeRowWriter.zeroOutNullBytes()

              if (col0.isNullAt(i)) {
                /* set null at zero */
                unsafeRowWriter.setNullAt(0)
              } else {
                /* otherwise we read and materialize */
                unsafeRowWriter.write(0, col0.getInt(i))
              }

              if (col1.isNullAt(i)) {
                /* set null at zero */
                unsafeRowWriter.setNullAt(1)
              } else {
                /* otherwise we read and materialize */
                unsafeRowWriter.write(1, col1.getBinary(i))
              }
              unsafeRow.setTotalSize(bufferHolder.totalSize())
              if(maxSize < bufferHolder.totalSize()) {
                maxSize = bufferHolder.totalSize()
              }
              //At this point the UnsafeRow is materialized
              // Adding to the list and how fast one can consume this iterator
              i += 1
              rows += 1
            }
          }
        } finally {
          reader.close()
        }
      }
      val diff = System.nanoTime() - start
      System.out.println(" counted rows in the UnsafeRow mode : " + rows + " time: " + diff / 1000 + " us, or " + diff / rows + " ns/Row size : " + maxSize)
    }
  }

  def readMyParquetFileLevel0(args:Array[String]): Unit = {
    //readMyParquetFileAsHadoopFiles
    System.out.println("================= [ Starting HDFS file reading experiment ] ================== ")
    val path = new Path(args(0))
    val conf = new Configuration()
    val fileSystem = path.getFileSystem(conf)

    val fileStatusAll:Array[FileStatus]  = fileSystem.listStatus(path)
    val filesAll = fileStatusAll.map(_.getPath).filter(ok).toList

    // this is hadoop reading benchmark
    var totalSize = 0L
    val byteArray = new Array[Byte](128 * 1024 *1024)

    val start = System.nanoTime()
    filesAll.foreach { p =>
      val fileLength = fileSystem.getFileStatus(p).getLen
      totalSize+=fileLength
      var readBytes = 0L
      val inStream = fileSystem.open(p)
      var rx = inStream.read(byteArray)
      while(rx != -1){
        readBytes += rx
        rx = inStream.read(byteArray)
      }
      System.out.println(p + " file finished, expectedBytes " + fileLength + " readBytes: " + readBytes)
    }
    val diff = System.nanoTime() - start
    val result = "[HDFS              ] time: " + diff / 1000 + " us, bandwidth " + (totalSize * 8L).toDouble / diff.toDouble + " Gbps | totalInputSize: " + totalSize + " bytes, in " + filesAll.length + " files\n"
    System.out.println(result)
    resultBuilder.append(result)
    this.totalInputHdfsBytes = totalSize
    System.out.println("================= [ Ending HDFS file reading experiment ] ================== ")
  }

  def readMyParquetFileLevel1(args:Array[String]) : Unit = {
    // at this level we try to consume iterator as fast as possible without UnsafeRow materilziation
    System.out.println("================= [ Starting parquet RowGroup reading experiment ] ================== ")
    val path = new Path(args(0))
    val conf = new Configuration()
    val fileSystem = path.getFileSystem(conf)

    val fileStatus:Array[FileStatus]  = fileSystem.listStatus(path)
    val files = fileStatus.map(_.getPath).filter(ok).toList
    var numPagesTotal = 0L

    val start = System.nanoTime()
    files.foreach { px =>
      var numPagesFile = 0L
      val fileLength: Long = fileSystem.getFileStatus(px).getLen
      val footer: ParquetMetadata = readFooter(conf, px, range(0, fileLength))
      val fileSchema = footer.getFileMetaData.getSchema
      val count = fileSchema.getFieldCount
      val schemaFields = scala.collection.mutable.ListBuffer.empty[String]
      for (a <- 0 until count) {
        schemaFields += fileSchema.getFieldName(a)
      }
      val blocksList = footer.getBlocks
      val reader: ParquetFileReader = new ParquetFileReader(conf, footer.getFileMetaData, px, blocksList, fileSchema.getColumns)
      var totalRowCount = 0L
      import scala.collection.JavaConversions._
      for (block <- blocksList) {
        totalRowCount += block.getRowCount
      }
      /* now we consume this in a loop */
      var soFar = 0L
      try {
        while (soFar < totalRowCount) {
          val pages: PageReadStore = reader.readNextRowGroup
          soFar+=pages.getRowCount
          numPagesFile+=1
        }
      } finally {
        System.out.println(px + " contains " + totalRowCount + " rows, and read " + soFar + " rows, number of RowGroups (or pages) : " + numPagesFile)
        numPagesTotal+=numPagesFile
        reader.close()
      }
    }
    val diff = System.nanoTime() - start
    val bw = this.totalInputHdfsBytes.toDouble * 8 / diff.toDouble
    val result = "[ParquetRowGroup   ] time: " + diff / 1000 + " us, bandwidth " + bw + " Gbps | time/Page : " + diff / numPagesTotal + " ns/Page, number of pages served : " + numPagesTotal+"\n"
    System.out.println(result)
    resultBuilder.append(result)
    System.out.println("================= [ Ending parquet RowGroup reading experiment ] ================== ")
  }

  def readMyParquetFileLevel2(args:Array[String]) : Unit = {
    System.out.println("================= [ Starting Spark ColumnarBatch reading experiment ] ================== ")
    // at this level we try to consume iterator as fast as possible without UnsafeRow materilziation
    val path = new Path(args(0))
    val conf = new Configuration()
    val fileSystem = path.getFileSystem(conf)

    val fileStatus:Array[FileStatus]  = fileSystem.listStatus(path)
    val files = fileStatus.map(_.getPath).filter(ok).toList
    var numBatches = 0L
    val schemaFields = scala.collection.mutable.ListBuffer.empty[String]
    schemaFields += "intKey"
    schemaFields += "payload"

    val start = System.nanoTime()
    files.foreach { p =>
      var perFileBatch = 0L
      val reader = new VectorizedParquetRecordReader()
      try {
        reader.initialize(p.toString, schemaFields.asJava)
        reader.enableReturningBatches()
        val batchIterator = new RecordReaderIterator(reader)

        while(batchIterator.hasNext){
          val colBatch = batchIterator.next().asInstanceOf[ColumnarBatch]
          colBatch.capacity()
          numBatches+=1
          perFileBatch+=1
        }
      } finally {
        System.out.println(p + " finished, num of ColumnarBatches are " + perFileBatch)
        reader.close()
      }
    }
    val diff = System.nanoTime() - start
    val bw = this.totalInputHdfsBytes.toDouble * 8 / diff.toDouble
    val result = "[SparkColumnarBatch] time: " + diff / 1000 + " us, bandwidth " + bw + " Gbps | time/Batch : " + diff / numBatches + " ns/Batch, number of batches served : " + numBatches + "\n"
    System.out.println(result)
    resultBuilder.append(result)
    System.out.println("================= [ Ending Spark ColumnarBatch reading experiment ] ================== ")
  }

  def readMyParquetFileViaIterator(args:Array[String]): Unit = {
    System.out.println("================= [ Starting Spark UnsafeRow reading experiment ] ================== ")
    val path = new Path(args(0))
    val conf = new Configuration()
    val fileSystem = path.getFileSystem(conf)

    val fileStatus:Array[FileStatus]  = fileSystem.listStatus(path)
    val files = fileStatus.map(_.getPath).filter(ok).toList
    val constructSchema = false

    // Decoding in vectorized but having the reader return UnsafeRow
    val start = System.nanoTime()
    var rows = 0L
    var totalSize = 0L
    var maxRowSize = 0L
    files.foreach { p =>

      val fileLength: Long = fileSystem.getFileStatus(p).getLen
      totalSize+=fileLength

      val schemaFields  = if(constructSchema) {
        val footer: ParquetMetadata = readFooter(conf, p, range(0, fileLength))
        val fileSchema = footer.getFileMetaData.getSchema
        val count = fileSchema.getFieldCount
        val schemaFields = scala.collection.mutable.ListBuffer.empty[String]
        for (a <- 0 until count) {
          schemaFields += fileSchema.getFieldName(a)
        }
        schemaFields
      } else {
        val schemaFields = scala.collection.mutable.ListBuffer.empty[String]
        schemaFields += "intKey"
        schemaFields += "payload"
        schemaFields
      }
      /* from there on we use the generated code */
      val vectorizedReader = new VectorizedParquetRecordReader
      vectorizedReader.initialize(p.toString, schemaFields.asJava)
      vectorizedReader.enableReturningBatches()

      val recordIterator = new RecordReaderIterator(vectorizedReader).asInstanceOf[Iterator[InternalRow]]
      val objArr = new Array[Object](2)
      objArr(0) = new SQLMetric("atr1", 0L)
      objArr(1) = new SQLMetric("atr1", 0L)
      val generatedIterator = new AtrGeneratedIterator(objArr)
      generatedIterator.init(0, Array(recordIterator))

      var itemCount = 0L
      while(generatedIterator.hasNext){
        val item = generatedIterator.next().asInstanceOf[UnsafeRow]
        val sz = item.getSizeInBytes
        if(sz > maxRowSize)
          maxRowSize = sz

        itemCount+=1
        rows+=1
      }
      System.out.println(p + " contains " + itemCount + " UnsafeRows ")
    }

    val diff = System.nanoTime() - start
    val bw = (totalSize * 8L).toDouble / diff.toDouble
    val result = "[UnsafeRow         ] time: " + diff / 1000 + " us, bandwidth " + bw + " Gbps | rows: " + rows + " , time/Row " + diff / rows + " ns/Row, maxRowSize: " + maxRowSize + " bytes, totalInputSize: " + totalSize + " bytes\n"
    System.out.println(result)
    resultBuilder.append(result)
    System.out.println("================= [ Ending Spark UnsafeRow reading experiment ] ================== ")
  }


  def readMyParquetFileViaIteratorSparkJob(fileName:String, spark:SparkSession): Unit = {
    System.out.println("================= [ *Starting Spark UnsafeRow reading experiment ] ================== ")

    val path = new Path(fileName)
    val conf = new Configuration()
    val fileSystem = path.getFileSystem(conf)
    // we get the file system

    val fileStatus:Array[FileStatus]  = fileSystem.listStatus(path)
    val files = fileStatus.map(_.getPath).filter(ok).toList
    val constructSchema = false
    // we have all the files now

    val inx = spark.sparkContext.parallelize(files)

    // Decoding in vectorized but having the reader return UnsafeRow
    val start = System.nanoTime()
    val totalRows = spark.sparkContext.longAccumulator("totalRows")
    val totalBytes = spark.sparkContext.longAccumulator("totalBytes")

    inx.foreach { p =>

      val fileLength: Long = fileSystem.getFileStatus(p).getLen
      totalBytes.add(fileLength)

      /* from there on we use the generated code */
      val vectorizedReader = new VectorizedParquetRecordReader
      vectorizedReader.initialize(p.toString, List("intKey", "payload").asJava)
      vectorizedReader.enableReturningBatches()

      val recordIterator = new RecordReaderIterator(vectorizedReader).asInstanceOf[Iterator[InternalRow]]
      val objArr = new Array[Object](2)
      // these are dummy SQL metrics we can remove them eventually
      objArr(0) = new SQLMetric("atr1", 0L)
      objArr(1) = new SQLMetric("atr1", 0L)
      val generatedIterator = new AtrGeneratedIterator(objArr)
      generatedIterator.init(0, Array(recordIterator))

      while(generatedIterator.hasNext){
        generatedIterator.next().asInstanceOf[UnsafeRow]
        totalRows.add(1L)
      }
    }
    val rows = totalRows.value
    val diff = System.nanoTime() - start
    val bw = (totalBytes.value * 8L).toDouble / diff.toDouble
    val result = "[UnsafeRow         ] time: " + diff / 1000 + " us, bandwidth " + bw + " Gbps | rows: " +  rows + " , time/Row " + diff / rows + " ns/Row , totalInputSize: " + totalBytes.value + " bytes\n"
    System.out.println(result)
    resultBuilder.append(result)
    System.out.println("================= [ Ending Spark UnsafeRow reading experiment ] ================== ")
  }


  def main(fileName:String, spark:SparkSession): Unit = {
    //TODO: parallelize this in a single executor with x number of cores?
    //readMyParquetFile(args)
    //readMyParquetFileLevel0(args) // performance of HDFS
    //readMyParquetFileLevel1(args) // peformance of reading orwGroups from parquet
    //readMyParquetFileLevel2(args) // performance of setting up ColumnarBatch
    //readMyParquetFileViaIterator(args) // performance of unsafe row
    readMyParquetFileViaIteratorSparkJob(fileName, spark)

    System.out.println("************* Results ******************************")
    System.out.println(resultBuilder.mkString+"\n")
    System.out.println("****************************************************")
  }
}
