package org.queue.bd.exercise1

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkWordCount {
  def main (args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    val rdd = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")

    val result = rdd
      .map(token => (token, 1))
      .reduceByKey(_+_)

    println(result.saveAsTextFile("/home/epierfederici/E3-scala/WordCount"))
  }
}