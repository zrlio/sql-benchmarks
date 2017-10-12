package com.ibm.crail.benchmarks

import com.fasterxml.jackson.databind.node.DoubleNode

/**
  * Created by atr on 11.10.17.
  */
object Utils {

  def sizeStrToBytes10(str: String): Long = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      lower.substring(0, lower.length - 1).toLong * 1000
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000 * 1000
    } else {
      // no suffix, so it's just a number in bytes
      lower.toLong
    }
  }

  def sizeToSizeStr10(size: Long): String = {
    val kbScale: Long = 1000
    val mbScale: Long = 1000 * kbScale
    val gbScale: Long = 1000 * mbScale
    val tbScale: Long = 1000 * gbScale
    if (size > tbScale) {
      size / tbScale + "TB"
    } else if (size > gbScale) {
      size / gbScale  + "GB"
    } else if (size > mbScale) {
      size / mbScale + "MB"
    } else if (size > kbScale) {
      size / kbScale + "KB"
    } else {
      size + "B"
    }
  }

  def sizeStrToBytes2(str: String): Long = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      lower.substring(0, lower.length - 1).toLong * 1024
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length - 1).toLong * 1024 * 1024
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length - 1).toLong * 1024 * 1024 * 1024
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length - 1).toLong * 1024 * 1024 * 1024 * 1024
    } else {
      // no suffix, so it's just a number in bytes
      lower.toLong
    }
  }

  def sizeToSizeStr2(size: Long): String = {
    val kbScale: Long = 1024
    val mbScale: Long = 1024 * kbScale
    val gbScale: Long = 1024 * mbScale
    val tbScale: Long = 1024 * gbScale
    if (size > tbScale) {
      size / tbScale + "TiB"
    } else if (size > gbScale) {
      size / gbScale  + "GiB"
    } else if (size > mbScale) {
      size / mbScale + "MiB"
    } else if (size > kbScale) {
      size / kbScale + "KiB"
    } else {
      size + "B"
    }
  }

  def decimalRound(value: Double):Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def twoLongDivToDecimal(dividend: Long, divisor:Long):Double = {
    decimalRound(dividend.toDouble / divisor.toDouble)
  }

  val MILLISEC = 1000L
  val MICROSEC = MILLISEC * 1000L
  val NANOSEC = MICROSEC * 1000L
}
