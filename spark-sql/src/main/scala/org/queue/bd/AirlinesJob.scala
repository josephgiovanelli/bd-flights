package org.queue.bd

import org.apache.spark.sql.SparkSession
import pojos.{Airline, Flight}

object AirlinesJob {


  case class YAAirline(iata_code: String, airline: String)
  case class YAFlight(airline: String, arrival_delay: Double)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL AirlinesJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val airlinesDF = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airlines")
      .map(x => new Airline(x))
      .map(x => YAAirline(x.getIata_code, x.getAirline)).toDF()

    val flightsDF = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      .map(x => new Flight(x))
      .map(x => YAFlight(x.getAirline, x.getArrival_delay.toDouble)).toDF()

    airlinesDF.createOrReplaceTempView("airlines")
    flightsDF.createOrReplaceTempView("flights")

    val summarizedFlightsDF = sqlContext.sql(
      """select airline, avg(arrival_delay) as average_delay
        |from flights
        |group by airline""".stripMargin)

    summarizedFlightsDF.createOrReplaceTempView("summarized_flights")
    summarizedFlightsDF.show()

    sqlContext.sql(
      """select A.airline, SF.average_delay
        |from summarized_flights SF
        |join airlines A on SF.airline = A.iata_code""".stripMargin)
      .coalesce(1)
      .sortWithinPartitions($"average_delay".desc)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/outputs/spark-sql/airlines")
  }
}
