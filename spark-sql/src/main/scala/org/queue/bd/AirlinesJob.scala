package org.queue.bd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import pojos.{Airline, Flight}

object AirlinesJob {


  case class YAAirline(iata_code: String, airline: String)
  case class YAFlight(airline: String, arrival_delay: Double)


  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Spark AirlinesJob"))
    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val airlinesDF = sc.textFile("hdfs:/user/jgiovanelli/flights/airlines.csv")
      .map(x => new Airline(x))
      .map(x => YAAirline(x.getIata_code, x.getAirline)).toDF()

    val flightsDF = sc.textFile("hdfs:/user/jgiovanelli/flights/flights.csv")
      .map(x => new Flight(x))
      .filter(x => x.getCancelled == "0" && x.getDiverted == "0")
      .map(x => YAFlight(x.getAirline, x.getArrival_delay.toDouble)).toDF()

    airlinesDF.registerTempTable("airlines")
    flightsDF.registerTempTable("flights")

    val summarizedFlightsDF = sqlContext.sql(
      """select airline, avg(arrival_delay) as average_delay
        |from flights
        |group by airline""".stripMargin)

    summarizedFlightsDF.registerTempTable("summarized_flights")
    summarizedFlightsDF.show()

    val resultDF = sqlContext.sql(
      """select A.airline, SF.average_delay
        |from summarized_flights SF
        |join airlines A on SF.airline = A.iata_code
        |order by SF.average_delay desc""".stripMargin)

    resultDF.show()
    resultDF.write.parquet("hdfs:/user/jgiovanelli/spark-sql/airlines.parquet")
  }
}
