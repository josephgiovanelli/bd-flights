package org.queue.bd

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import pojos.{Airport, Flight}
import utils.TimeSlot

/**
  * SparkSQL job to list the airports in each time slot ordered descendingly by the average taxi out delay.
  */
object AirportsJob {

  //case classes used to store the data
  case class YAAirport(iata_code: String, airport: String)
  case class YAFlight(origin_airport: String, time_slot: String, taxi_out: Double)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL AirportsJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sqlContext.setConf("spark.sql.shuffle.partitions", "20")
    import sqlContext.implicits._

    //loading the airports data set from the hdfs
    val airportsDF = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airports")
      //using common.pojos
      .map(x => new Airport(x))
      //selecting the fields of interest
      .map(x => YAAirport(x.getIata_code, x.getAirport)).toDF()

    //loading the flights data set from the hdfs
    val flightsDF = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      //using common.pojos
      .map(x => new Flight(x))
      //selecting the fields of interest
      .map(x => YAFlight(x.getOrigin_airport, TimeSlot.getTimeSlot(x.getScheduled_departure).getDescription, x.getTaxi_out.toDouble)).toDF()

    flightsDF
      //computing the taxi out average delay
      .groupBy("origin_airport", "time_slot").avg("taxi_out")
      //merging the above data sets
      .join(broadcast(airportsDF), flightsDF("origin_airport") === airportsDF("iata_code"))
      //selecting the fields of interest
      .select("airport", "time_slot", "avg(taxi_out)")
      .repartition(1)
      //sorting them
      .sortWithinPartitions($"avg(taxi_out)".desc)
      //writing the results on a file
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/outputs/spark-sql/airports")
  }
}
