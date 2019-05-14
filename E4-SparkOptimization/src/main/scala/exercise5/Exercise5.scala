package exercise5

import materiale.{StationData, WeatherData}
import org.apache.spark.{SparkConf, SparkContext}


class Exercise5 extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  ////// Setup

  val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
  val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

  ////// Exercise 5

  // Clean the cache
  sc.getPersistentRDDs.foreach(_._2.unpersist())

  // Use Spark's web UI to verify the space occupied by the following RDDs:
  import org.apache.spark.storage.StorageLevel._
  val memRdd = rddWeather.sample(false,0.1).repartition(8).cache()
  val memSerRdd = memRdd.map(x=>x).persist(MEMORY_ONLY_SER)
  val diskRdd = memRdd.map(x=>x).persist(DISK_ONLY)

  memRdd.collect()
  memSerRdd.collect()
  diskRdd.collect()

  /*
   * La serializzazione occupa meno spazio in memorizzazione, ma produce un maggior costo CPU
   * e quindi il Tradeoff è costi-cpu
   */

}