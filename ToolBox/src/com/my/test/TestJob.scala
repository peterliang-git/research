package com.my.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object TestJob {
  def main(arg: Array[String]) : Unit = {
    println("Hello, Scala");
    //Create conf object
    val conf = new SparkConf()
      .setAppName("WordCount")

    //create spark context object
    val sc = new SparkContext(conf)

    //Check whether sufficient params are supplied
    if (arg.length < 2) {
      println("Usage: ScalaWordCount <input> <output>")
      System.exit(1)
    }
    //Read file and create RDD
    val rawData = sc.textFile(arg(0))
    val scalaRDD = rawData.filter(line => line.contains("scala"))
    println("Input had " + scalaRDD.count() + " concerning lines")
    println("Here are 10 examples:")
    scalaRDD.take(10).foreach(println)
    
    //convert the lines into words using flatMap operation
    val words = rawData.flatMap(line => line.split(" "))

    //count the individual words using map and reduceByKey operation
    val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)

    //Save the result
    wordCount.saveAsTextFile(arg(1))
    
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input map (x=>x*x)
    println(result.collect().mkString(","))

    //stop the spark context
    sc.stop
  }

}