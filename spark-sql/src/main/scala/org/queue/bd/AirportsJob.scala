package org.queue.bd

import org.apache.spark.sql.SparkSession
import pojos.{Airport, Flight}
import utils.TimeSlot

object AirportsJob {


  case class YAAirport(iata_code: String, airport: String)
  case class YAFlight(origin_airports: String, time_slot: String, taxi_out: Double)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark AirportsJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val airportsDF = sc.textFile("hdfs:/user/jgiovanelli/flights/airports.csv")
      .map(x => new Airport(x))
      .map(x => YAAirport(x.getIata_code, x.getAirport)).toDF()

    val flightsDF = sc.textFile("hdfs:/user/jgiovanelli/flights/flights.csv")
      .map(x => new Flight(x))
      .filter(x => x.getCancelled == "0" && x.getDiverted == "0")
      .map(x => YAFlight(x.getOrigin_airport, TimeSlot.getTimeSlot(x.getDeparture_time).getDescription, x.getTaxi_out.toDouble)).toDF()

    airportsDF.createOrReplaceTempView("airport")
    flightsDF.createOrReplaceTempView("flights")

    val summarizedFlightsDF = flightsDF.groupBy("origin_airport", "time_slot").avg("taxi_out")

    summarizedFlightsDF.createOrReplaceTempView("summarized_flights")
    summarizedFlightsDF.show()

    summarizedFlightsDF
      .join(airportsDF, summarizedFlightsDF("origin_airport") === airportsDF("iata_code"))
      .select("airport", "time_slot", "avg(taxi_out)")
      .repartition(1)
      .sortWithinPartitions($"avg(taxi_out)".desc)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/spark-sql/airports.csv")

  }
}
