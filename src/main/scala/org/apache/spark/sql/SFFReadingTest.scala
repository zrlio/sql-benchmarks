package org.apache.spark.sql

import java.io.EOFException

import com.ibm.crail.{CrailFS, CrailFile}
import com.ibm.crail.benchmarks.SQLTest
import com.ibm.crail.conf.CrailConfiguration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
  * Created by atr on 22.09.17.
  */

class SFFReadingTest(val item:(List[String], Long) , spark:SparkSession) extends SQLTest(spark) {

  private class FastIterator(fileName: String, numFields: Int) extends Iterator[InternalRow] {
    private val conf = new CrailConfiguration
    private val cfs = CrailFS.newInstance(conf)
    private val fx: CrailFile = cfs.lookup(new Path(fileName).toUri.getRawPath).get().asFile()
    private val stream = fx.getBufferedInputStream(fx.getCapacity)

    private val unsafeRow = new UnsafeRow(2)
    private var done = false
    private var incomingSize = 0
    private var buffer: Array[Byte] = _

    def readNext(): Unit = {
      try {
        incomingSize = stream.readInt()
        if (incomingSize == -1) {
          done = true
          this.stream.close()
        }
      } catch {
        case e: EOFException => {
          /* we mark EOF */
          done = true
          this.stream.close()
        }
        case e1: Exception => throw e1
      }
      if (!done) {
        if (buffer == null || buffer.length < incomingSize) {
          /* we resize the buffer */
          buffer = new Array[Byte](incomingSize)
        }
        /* then we read the next value */
        this.stream.read(buffer, 0, incomingSize)
        unsafeRow.pointTo(buffer, incomingSize)
      }
    }

    override def hasNext(): Boolean = {
      readNext()
      !done
    }

    override def next(): InternalRow = {
      unsafeRow
    }
  }
  def filterSFFMetaFiles(f:String):Boolean = {
    f.substring(f.size - "-mdata".size, f.size).compareTo("-mdata") != 0
  }

  /* Rdd of iterator type ? how does crail streams behave for serialization? */

  val rdd = spark.sparkContext.parallelize(item._1.filter(filterSFFMetaFiles))
  val totalRows = spark.sparkContext.longAccumulator("totalRows")

  override def execute(): String = {
    rdd.foreach(f => {
      val itr = new FastIterator(f, 2)
      while (itr.hasNext()) {
        val nx = itr.next()
        totalRows.add(1L)
      }
    })
    " fast iterator consumed "
  }

  override def explain(): Unit = {}

  override def plainExplain(): String = " read " + item._2 + " bytes in " + totalRows.value + " rows"
}
