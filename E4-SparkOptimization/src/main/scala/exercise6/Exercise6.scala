package exercise6

import materiale.{StationData, WeatherData}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


class Exercise6 extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  ////// Setup

  val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
  val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

  ////// Exercise 6

  val rddW = rddWeather.sample(false,0.1).filter(_.temperature<999).keyBy(x => x.usaf + x.wban).cache()
  val rddS = rddStation.keyBy(x => x.usaf + x.wban).partitionBy(new HashPartitioner(8)).cache()
  rddW.collect
  rddS.collect

  // Is it better to simply join the two RDDs..
  rddW
    .join(rddS)
    .filter(_._2._2.country=="IT")
    .map({case(k,v)=>(v._2.name,v._1.temperature)})
    .reduceByKey((x,y)=>{if(x<y) y else x})
    .collect()

  // ..to enforce on rddW1 the same partitioner of rddS..
  rddW
    .partitionBy(new HashPartitioner(8))
    .join(rddS)
    .filter(_._2._2.country=="IT")
    .map({case(k,v)=>(v._2.name,v._1.temperature)})
    .reduceByKey((x,y)=>{if(x<y) y else x})
    .collect()

  // ..or to exploit broadcast variables?
  val bRddS = sc.broadcast(rddS.collectAsMap())
  val rddJ = rddW
    .map({case (k,v) => (bRddS.value.get(k),v)})
    .filter(_._1!=None)
    .map({case(k,v)=>(k.get.asInstanceOf[StationData],v)})
  rddJ
    .filter(_._1.country=="IT")
    .map({case (k,v) => (k.name,v.temperature)})
    .reduceByKey((x,y)=>{if(x<y) y else x})
    .collect()

  /*
   * Secondo: peggiore (devi prima partizionare e poi rifare shuffle se dati non sono in partizioni corrispondenti su stesso executor fisico)
   * Terzo: migliore (deve solo riaccedere in memoria)
   */
}