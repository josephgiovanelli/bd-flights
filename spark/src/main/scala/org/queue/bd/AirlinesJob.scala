package org.queue.bd

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import pojos.{Airline, Flight}
import org.queue.bd.RDDUtils._

object AirlinesJob {

  def toCSVLine(data: (String, Double)): String = data._1 + "," + data._2.toString


  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark AirlinesJob"))

    val rddAirlines = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airlines")
      .map(x => new Airline(x))
      .map(x => (x.getIata_code, x.getAirline))

    val rddFlights = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      .map(x => new Flight(x))
      .map(x => (x.getAirline, x.getArrival_delay.toDouble))
      .partitionBy(new HashPartitioner(116))
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k, v._1 / v._2) })

    val rddResult = rddFlights
      .join(rddAirlines)
      .map({ case (k, v) => (v._2, v._1) })
      .sortBy(_._2, ascending = false, numPartitions = 1)
      .cache()

    rddResult.collect()

    rddResult
      .map(x => toCSVLine(x))
      .overwrite("hdfs:/user/jgiovanelli/outputs/spark/airlines")

  }
}
