package exercise4

import materiale.{StationData, WeatherData}
import org.apache.spark.{SparkConf, SparkContext}


class Exercise4 extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  ////// Setup

  val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
  val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

  ////// Exercise 4

  // Given Exercise 3, compute the maximum temperature for every city
  // Given Exercise 3, compute the maximum temperature for every city in Italy
  StationData.country == "IT"
  // Sort the results by descending temperature
  map({case(k,v)=>(v,k)})

}