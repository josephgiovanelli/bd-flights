package org.queue.bd

import org.apache.spark.{SparkConf, SparkContext}
import pojos.{Airline, Flight}

object AirlinesJob {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark AirlinesJob"))

    val rddAirlines = sc.textFile("hdfs:/user/jgiovanelli/flights/airlines.csv")
      .map(x => new Airline(x))
      .map(x => (x.getIata_code, x.getAirline))

    val rddFlights = sc.textFile("hdfs:/user/jgiovanelli/flights/flights.csv")
      .map(x => new Flight(x))
      .filter(x => x.getCancelled == "0" && x.getDiverted == "0")
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

    rddJoined.collect()
    rddJoined.saveAsTextFile("hdfs:/user/jgiovanelli/spark/airlines")
  }
}
