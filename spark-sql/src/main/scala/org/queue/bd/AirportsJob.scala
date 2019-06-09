package org.queue.bd

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import pojos.{Airport, Flight}
import utils.TimeSlot

object AirportsJob {


  case class YAAirport(iata_code: String, airport: String)
  case class YAFlight(origin_airport: String, time_slot: String, taxi_out: Double)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL AirportsJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sqlContext.setConf("spark.sql.shuffle.partitions", "20")
    import sqlContext.implicits._

    val airportsDF = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airports")
      .map(x => new Airport(x))
      .map(x => YAAirport(x.getIata_code, x.getAirport)).toDF()

    val flightsDF = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      .map(x => new Flight(x))
      .map(x => YAFlight(x.getOrigin_airport, TimeSlot.getTimeSlot(x.getScheduled_departure).getDescription, x.getTaxi_out.toDouble)).toDF()

    flightsDF.groupBy("ORIGIN_AIRPORT", "TIME_SLOT").avg("TAXI_OUT")
      .join(broadcast(airportsDF), flightsDF("ORIGIN_AIRPORT") === airportsDF("IATA_CODE"))
      .select("AIRPORT", "TIME_SLOT", "avg(TAXI_OUT)")
      .repartition(1)
      .sortWithinPartitions($"avg(TAXI_OUT)".desc)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/outputs/spark-sql/airports")
  }
}
