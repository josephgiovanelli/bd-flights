package org.queue.bd

import org.apache.spark.{SparkConf, SparkContext}
import org.queue.bd.RDDUtils._
import pojos.{Airport, Flight}
import utils.TimeSlot


object AirportsJob {


  def toCSVLine(data: ((String, String), Double)): String =
    data._1._1 + "," + data._1._2 + "," + data._2.toString

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark AirportsJob with BV"))

    val rddAirports = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airports")
      .map(x => new Airport(x))
      .map(x => (x.getIata_code, x.getAirport))

    val rddFlights = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      .map(x => new Flight(x))
      .map(x => ((x.getOrigin_airport, TimeSlot.getTimeSlot(x.getScheduled_departure).getDescription), x.getTaxi_out.toDouble))
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k._1, (k._2, v._1 / v._2)) })

    val broadcastRddAirports = sc.broadcast(rddAirports.collectAsMap())

    val rddResult = rddFlights
    .map({ case (k, v) => (broadcastRddAirports.value.get(k), v) })
    .filter(_._1.isDefined)
    .map({ case (k, v) => ((k.get, v._1), v._2) })
    .sortBy(_._2, ascending = false, numPartitions = 1)
    .cache()

    rddResult.collect()

    rddResult.map(x => toCSVLine(x))
             .overwrite("hdfs:/user/jgiovanelli/outputs/spark/airports")

  }
}
