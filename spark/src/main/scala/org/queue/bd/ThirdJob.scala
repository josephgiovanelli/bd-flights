package org.queue.bd

import org.apache.spark.{SparkConf, SparkContext}
import pojos.{Airline, Airport, Flight}
import utils.TimeSlot

object ThirdJob {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark AirlinesJob"))

    //Loading Part
    val rddAirlines = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airlines")
      .map(x => new Airline(x))
      .map(x => (x.getIata_code, x.getAirline))

    val rddAirports = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airports")
      .map(x => new Airport(x))
      .map(x => (x.getIata_code, x.getAirport))

    val rddCommonFlights = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      .map(x => new Flight(x))
      .cache()

    //Airlines Part
    val rddFlightsForAirlines = rddCommonFlights
      .map(x => (x.getAirline, x.getArrival_delay.toDouble))
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k, v._1 / v._2) })

    val broadcastRddAirlines = sc.broadcast(rddAirlines.collectAsMap())

    val rddJoinedForAirlines = rddFlightsForAirlines
      .map({ case (k, v) => (broadcastRddAirlines.value.get(k), v) })
      .filter(_._1.isDefined)
      .map({ case (k, v) => (k.get, v) })
      .sortBy(_._2, ascending = false)
      .coalesce(1)
      .cache()

    def toCSVLineForAirlines(data: (String, Double)): String = data._1 + "," + data._2.toString

    val rddResultForAirlines = rddJoinedForAirlines.map(x => toCSVLineForAirlines(x))
    rddResultForAirlines.collect()
    rddResultForAirlines.saveAsTextFile("hdfs:/user/jgiovanelli/outputs/spark/third-job/airlines")

    //Airports Part
    val rddFlightsForAirports = rddCommonFlights
      .map(x => ((x.getOrigin_airport, TimeSlot.getTimeSlot(x.getDeparture_time)), x.getTaxi_out.toDouble))
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k._1, (k._2, v._1 / v._2)) })
      .join(rddAirports)
      .map({ case (k, v) => ((v._2, v._1._1), v._1._2) })
      .sortBy(_._2, ascending = false)
      .coalesce(1)
      .cache()

    def toCSVLineForAirports(data: ((String, TimeSlot), Double)): String =
      (data._1)._1 + "," + (data._1)._2.getDescription + "," + data._2.toString

    val rddResultForAirports = rddFlightsForAirports.map(x => toCSVLineForAirports(x))
    rddResultForAirports.collect()
    rddResultForAirports.saveAsTextFile("hdfs:/user/jgiovanelli/outputs/spark/third-job/airports")


  }
}