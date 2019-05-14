package org.queue.bd.exercise3

import materiale.{StationData, WeatherData}
import org.apache.spark.{SparkConf, SparkContext}


class Exercise3 extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  ////// Setup

  val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
  val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

  ////// Exercise 3

  // Join the Weather and Station RDDs
  rddWeather.filter(_.temperature<999)
  rdd1.join(rdd2)
  // Hints & considerations:
  // - Join syntax: rdd1.join(rdd2)
  // - Both RDDs should be structured as key-value RDDs with the same key: usaf + wban
  // - Consider partitioning and caching to optimize the join
  // - Careful: it is not enough for the two RDDs to have the same number of partitions; they must have the same partitioner!
  // - Verify the execution plan of the join in the web UI

}