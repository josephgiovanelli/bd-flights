package org.queue.bd

import org.apache.spark.{SparkConf, SparkContext}
import pojos.{Airline, Airport, Flight}
import utils.TimeSlot
import org.queue.bd.RDDUtils._


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
      .sortBy(_._2, ascending = false, numPartitions = 1)
      .cache()

    def toCSVLineForAirlines(data: (String, Double)): String = data._1 + "," + data._2.toString

    val rddResultForAirlines = rddJoinedForAirlines.map(x => toCSVLineForAirlines(x))
    rddResultForAirlines.collect()
    rddResultForAirlines.overwrite("hdfs:/user/jgiovanelli/outputs/spark/third-job/airlines")

    //Airports Part
    val rddFlightsForAirports = rddCommonFlights
      .map(x => ((x.getOrigin_airport, TimeSlot.getTimeSlot(x.getScheduled_departure).getDescription), x.getTaxi_out.toDouble))
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k._1, (k._2, v._1 / v._2)) })

    val broadcastRddAirports = sc.broadcast(rddAirports.collectAsMap())

    val rddJoinedForAirports = rddFlightsForAirports
      .map({ case (k, v) => (broadcastRddAirports.value.get(k), v) })
      .filter(_._1.isDefined)
      .map({ case (k, v) => ((k.get, v._1), v._2) })
      .sortBy(_._2, ascending = false, numPartitions = 1)
      .cache()

    def toCSVLineForAirports(data: ((String, String), Double)): String =
      data._1._1 + "," + data._1._2 + "," + data._2.toString

    val rddResultForAirports = rddJoinedForAirports.map(x => toCSVLineForAirports(x))
    rddResultForAirports.collect()
    rddResultForAirports.overwrite("hdfs:/user/jgiovanelli/outputs/spark/third-job/airports")

  }
}