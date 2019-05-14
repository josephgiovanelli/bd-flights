package exercise2

import materiale.{StationData, WeatherData}
import org.apache.spark.{SparkConf, SparkContext}


class Exercise2 extends App {
  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  ////// Setup

  val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
  val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

  ////// Exercise 2

  import org.apache.spark.HashPartitioner
  val p = new HashPartitioner(8)

  // Consider the following commands to transform the Station RDD:
  val rddS1 = rddStation.partitionBy(p).keyBy(x => x.usaf + x.wban).cache()
  val rddS2 = rddStation.partitionBy(p).cache().keyBy(x => x.usaf + x.wban)
  val rddS3 = rddStation.keyBy(x => x.usaf + x.wban).partitionBy(p).cache()
  val rddS4 = rddStation.keyBy(x => x.usaf + x.wban).cache().partitionBy(p)
  // Which of these options is better? And why?

  // rddS3: non ha cachare prima del ripartizionamento, altrimenti ogni volta va ripetuto lo shuffeling di ripartizionamento
  // A rigor di logica la cache va messa più lontano possibile (in fondo)
  // A rigor di logica partition by va fatto dopo aver strutturato l'rdd come chiave-valore
  // Ogni operazione che modifica la chiave ha senso farla PRIMA del partizionamento
}