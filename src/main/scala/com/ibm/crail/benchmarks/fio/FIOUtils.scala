package com.ibm.crail.benchmarks.fio

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SimpleFileFormat, SparkSession}

/**
  * Created by atr on 12.10.17.
  */
object FIOUtils {

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
      println("processing path: " + p)
      _process(p)
    }).reduce((i1, i2) => (i1._1 ++ i2._1, i1._2 + i2._2))
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

  def enumerateWithSize(fileName:String):List[(String, Long)] = {
    val path = new Path(fileName)
    val conf = new Configuration()
    val fileSystem = path.getFileSystem(conf)
    // we get the file system
    val fileStatus:Array[FileStatus]  = fileSystem.listStatus(path)
    val files = fileStatus.map(_.getPath).filter(ok).toList
    files.map(fx=> (fx.toString, fileSystem.getFileStatus(fx).getLen))
  }

  def inferSFFSchema(dirName:String, spark:SparkSession):Option[StructType] = {
    val path = new Path(dirName)
    val conf = new Configuration()
    val fileSystem = path.getFileSystem(conf)
    // we get the file system
    val fileStatus:Array[FileStatus]  = fileSystem.listStatus(path)
    val sff = new SimpleFileFormat
    sff.inferSchema(spark, Map[String, String](), fileStatus)
  }
}
