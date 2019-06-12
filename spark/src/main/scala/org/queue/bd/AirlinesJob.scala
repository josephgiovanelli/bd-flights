package org.queue.bd

import org.apache.spark.{SparkConf, SparkContext}
import org.queue.bd.RDDUtils._
import pojos.{Airline, Flight}

/**
  * Spark job to list the airlines ordered descendingly on the average arrival delay.
  */
object AirlinesJob {

  //Formats the tuple to a csv row
  def toCSVLine(data: (String, Double)): String = data._1 + "," + data._2.toString

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark AirlinesJob"))

    //loading the airlines data set from the hdfs
    val rddAirlines = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airlines")
      //using common.pojos
      .map(x => new Airline(x))
      //selecting the fields of interest
      .map(x => (x.getIata_code, x.getAirline))

    //loading the flights data set from the hdfs
    val rddFlights = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      //using common.pojos
      .map(x => new Flight(x))
      //selecting the fields of interest
      .map(x => (x.getAirline, x.getArrival_delay.toDouble))
      //computing the arrival average delay
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k, v._1 / v._2) })

    //merging the above data sets
    val rddResult = rddFlights
      .join(rddAirlines)
      .map({ case (k, v) => (v._2, v._1) })
      //and sorting them
      .sortBy(_._2, ascending = false, numPartitions = 1)
      .cache()

    //showing the results on console
    rddResult.collect().foreach(x => println(x))

    //writing the results on a file
    rddResult
      .map(x => toCSVLine(x))
      .overwrite("hdfs:/user/jgiovanelli/outputs/spark/airlines")

  }
}
