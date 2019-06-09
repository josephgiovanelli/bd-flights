package org.queue.bd

import org.apache.spark.sql.SparkSession

object PreprocessingJob {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL PreprocessingJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    //Flights
    sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("hdfs:/user/jgiovanelli/flights-dataset/raw/flights.csv")
      .filter(x => x.getAs[String]("CANCELLED") == "0" && x.getAs[String]("DIVERTED") == "0" &&
        x.getAs[String]("ORIGIN_AIRPORT").length == 3)
      .select("AIRLINE", "ORIGIN_AIRPORT", "SCHEDULED_DEPARTURE", "TAXI_OUT", "ARRIVAL_DELAY")
      .repartition(5)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")

    //Airlines
    sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("hdfs:/user/jgiovanelli/flights-dataset/raw/airlines.csv")
      .repartition(1)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/flights-dataset/clean/airlines")

    //Airports
    sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("hdfs:/user/jgiovanelli/flights-dataset/raw/airports.csv")
      .select("IATA_CODE", "AIRPORT")
      .repartition(1)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/flights-dataset/clean/airports")

  }
}
