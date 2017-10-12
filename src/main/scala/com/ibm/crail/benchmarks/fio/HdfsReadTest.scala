package com.ibm.crail.benchmarks.fio

import java.util.concurrent.ConcurrentHashMap

import com.ibm.crail.benchmarks.{BaseTest, FIOOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
  * Created by atr on 11.10.17.
  */
class HdfsReadTest(fioOptions:FIOOptions, spark:SparkSession) extends BaseTest {

  private val conf = new Configuration()
  private val fsMap = new ConcurrentHashMap[String, FileSystem]()
  private val defaultFS:FileSystem = FileSystem.get(new Path("/").toUri, conf)

  private val pathString = fioOptions.getInputLocations
  private val p = new Path(pathString)

  private def getFileSystem(path:Path):FileSystem = {
    val uri = path.toUri
    if(uri.getScheme == null || uri.getAuthority == null){
      defaultFS
    } else {
      /* we first look up */
      val key = uri.getScheme+":"+uri.getAuthority
      var lookupFS = fsMap.getOrDefault(key, null)
      if(lookupFS == null){
        /* we get the FS and make an entry */
        lookupFS = FileSystem.get(path.toUri, conf)
        this.fsMap.put(key, lookupFS)
        require(this.fsMap.size() < 4, " number of entries exceed 4? Are we using 4 HDFS file systems?. There is nothing wrong, I just wanted to check")
      }
      lookupFS
    }
  }

  override def execute(): String = ???

  override def explain(): Unit = ???

  override def plainExplain(): String = ???
}
