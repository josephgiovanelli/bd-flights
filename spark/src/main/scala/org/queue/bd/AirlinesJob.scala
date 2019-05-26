package org.queue.bd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pojos.{Airline, Flight}

object AirlinesJob {



  implicit class RichRDD[T](rDD: RDD[T]) {

    def overwrite(path: String): Unit = {

      def _deleteFolder(path: String): Unit = {
        try {
          FileSystem.get(new java.net.URI("hdfs://localhost:9000"), new Configuration())
            .delete(new Path(path), true)
        } catch {
          case _: Throwable =>
        }
      }
      
      _deleteFolder(path)
      rDD.saveAsTextFile(path)
    }
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark AirlinesJob"))

    val rddAirlines = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airlines")
      .map(x => new Airline(x))
      .map(x => (x.getIata_code, x.getAirline))

    val rddFlights = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      .map(x => new Flight(x))
      .map(x => (x.getAirline, x.getArrival_delay.toDouble))
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k, v._1 / v._2) })

    val broadcastRddAirlines = sc.broadcast(rddAirlines.collectAsMap())

    val rddJoined = rddFlights
      .map({ case (k, v) => (broadcastRddAirlines.value.get(k), v) })
      .filter(_._1.isDefined)
      .map({ case (k, v) => (k.get, v) })
      .sortBy(_._2, ascending = false)
      .coalesce(1)
      .cache()

    def toCSVLine(data: (String, Double)): String = data._1 + "," + data._2.toString

    val rddResult = rddJoined.map(x => toCSVLine(x))
    rddResult.collect()
    rddResult.overwrite("hdfs:/user/jgiovanelli/outputs/spark/airlines")

  }
}
