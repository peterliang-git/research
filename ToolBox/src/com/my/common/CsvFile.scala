package com.my.common

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

abstract class CsvFile[T: ClassTag](fileName: String, delimiter: String = ",", removeHead: Boolean = true) extends Loadable[T] with Serializable {
  protected val fieldDelimitor: String = delimiter
  protected val removeHeader: Boolean = removeHead

  def load(ss:SparkSession): RDD[T] = {
    val fields = ss.sparkContext.textFile(fileName).map(line => line.split(fieldDelimitor, -1))
    val body = removeHeader match {
      case true => fields.mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
      case _ => fields
    }
    body.map(x => parse(x))
  }

  protected def parse(as: Array[String]): T
}
