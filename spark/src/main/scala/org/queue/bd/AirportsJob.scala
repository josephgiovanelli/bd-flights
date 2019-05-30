package org.queue.bd

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import pojos.{Airport, Flight}
import utils.TimeSlot
import org.queue.bd.RDDUtils._

import org.apache.spark.Partitioner

class CustomPartitioner(numberOfPartitioner: Int) extends Partitioner {
  override def numPartitions: Int = numberOfPartitioner
  override def getPartition(key: Any): Int = {
    Math.abs(key.asInstanceOf[(String, TimeSlot)]._1.hashCode() % numPartitions)
  }
  // Java equals method to let Spark compare our Partitioner objects
  override def equals(other: Any): Boolean = other match {
    case partitioner: CustomPartitioner =>
      partitioner.numPartitions == numPartitions
    case _ =>
      false
  }
}

object AirportsJob {

  def toCSVLine(data: ((String, TimeSlot), Double)): String =
    data._1._1 + "," + data._1._2.getDescription + "," + data._2.toString

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark AirportsJob"))

    val rddAirports = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airports")
      .map(x => new Airport(x))
      .map(x => (x.getIata_code, x.getAirport))

    val rddFlights = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      .map(x => new Flight(x))
      .map(x => ((x.getOrigin_airport, TimeSlot.getTimeSlot(x.getDeparture_time)), x.getTaxi_out.toDouble))
      .partitionBy(new CustomPartitioner(116))
      .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
      .map({ case (k, v) => (k._1, (k._2, v._1 / v._2)) })

    val rddResult = rddFlights
      .join(rddAirports)
      .map({ case (k, v) => ((v._2, v._1._1), v._1._2) })
      .sortBy(_._2, ascending = false, numPartitions = 1)
      .cache()

    rddResult.collect()

    rddResult.map(x => toCSVLine(x))
             .overwrite("hdfs:/user/jgiovanelli/outputs/spark/airports")
  }
}
