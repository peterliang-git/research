package com.my.common

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

trait Loadable [T] {
  def load(ss: SparkSession): RDD[T]
}