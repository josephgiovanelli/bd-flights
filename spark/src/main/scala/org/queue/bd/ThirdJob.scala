package org.queue.bd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import pojos.{Airline, Airport, Flight}
import utils.TimeSlot
import org.queue.bd.RDDUtils._


object ThirdJob {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark ThirdJob")
    conf.registerKryoClasses(Array(classOf[Flight]))
    val sc = new SparkContext(conf)

    //Loading Part
    val rddAirlines = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airlines")
      .map(x => new Airline(x))
      .map(x => (x.getIata_code, x.getAirline))

    val rddAirports = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airports")
      .map(x => new Airport(x))
      .map(x => (x.getIata_code, x.getAirport))

    val rddCommonFlights = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      .map(x => new Flight(x))
      //.map(x => ((x.getAirline, x.getOrigin_airport), (x.getScheduled_departure, x.getArrival_delay, x.getTaxi_out)))
      //.partitionBy(new HashPartitioner(116))
      .repartition(1)
      .persist(StorageLevel.MEMORY_ONLY_SER)

    //rddCommonFlights.count



    //Airlines Part
    def toCSVLineForAirlines(data: (String, Double)): String = data._1 + "," + data._2.toString

    rddCommonFlights
      .map(x => (x.getAirline, x.getArrival_delay.toDouble))
      //.map({ case (k, v) => (k._1, v._2.toDouble) })
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k, v._1 / v._2) })
      .join(rddAirlines)
      .map({ case (k, v) => (v._2, v._1) })
      .sortBy(_._2, ascending = false, numPartitions = 1)
      .map(x => toCSVLineForAirlines(x))
      .overwrite("hdfs:/user/jgiovanelli/outputs/spark/third-job/airlines")

    //Airports Part
    def toCSVLineForAirports(data: ((String, String), Double)): String =
      data._1._1 + "," + data._1._2 + "," + data._2.toString

    rddCommonFlights
      .map(x => ((x.getOrigin_airport, TimeSlot.getTimeSlot(x.getScheduled_departure).getDescription), x.getTaxi_out.toDouble))
      //.map({ case (k, v) => ((k._2, TimeSlot.getTimeSlot(v._1).getDescription), v._3.toDouble) })
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k._1, (k._2, v._1 / v._2)) })
      //.partitionBy(new HashPartitioner(116))
      .join(rddAirports)
      .map({ case (k, v) => ((v._2, v._1._1), v._1._2) })
      .sortBy(_._2, ascending = false, numPartitions = 1)
      .map(x => toCSVLineForAirports(x))
      .overwrite("hdfs:/user/jgiovanelli/outputs/spark/third-job/airports")

  }
}