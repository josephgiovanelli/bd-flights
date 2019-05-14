package exercise1

import materiale.{StationData, WeatherData}
import org.apache.spark.{SparkConf, SparkContext}

class Exercise1 extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  ////// Setup

  val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
  val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

  ////// Exercise 1

  // We want the average AND the maximum temperature registered for every month
  val cached1 = rddWeather
    // Partizioni: di solito 2 o 3 per ogni core
    // N di partizioni dipende da partizioni del file
//    .repartition(5)
    .coalesce(5)
    .filter(_.temperature<999)
    .map(x => (x.month, x.temperature))
    .cache()

  cached1.aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1),(a1,a2)=>(a1._1+a2._1,a1._2+a2._2))
    .map({case(k,v)=>(k,v._1/v._2)})
    .collect()
  cached1.reduceByKey((x,y)=>{if(x<y) y else x})
    .collect()
  // Optimize the two jobs by avoiding the repetition of the same computations and by defining a good number of partitions
  // Hints:
  // - Verify your persisted data in the web UI
  // - Use either repartition() or coalesce() to define the number of partitions
  // - repartition() shuffles all the data
  // - coalesce() minimizes data shuffling by exploiting the existing partitioning
  // - Verify the execution plan of your RDDs with rdd.toDebugString
}