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

    val airportsDF =  {
      val airportsFile = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airports")
      val schemaString = "IATA_CODE AIRPORT"

      val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false)))
      val rowRDD = airportsFile.map(_.split(",")).map(e => Row(e(0), e(1)))
      sqlContext.createDataFrame(rowRDD, schema)
    }

    val flightsDF = {
      val flightsFile = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      val schemaString = "AIRLINE ORIGIN_AIRPORT SCHEDULED_DEPARTURE TAXI_OUT_TEMP ARRIVAL_DELAY"

      val getTimeSlot = sqlContext.udf.register("getTimeSlot", (s: String) =>  TimeSlot.getTimeSlot(s).getDescription)
      val stringToDouble = sqlContext.udf.register("stringToDouble", (s: String) =>  s.toDouble )

      val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false)))
      val rowRDD = flightsFile.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4)))
      val tempFlightsDF = sqlContext.createDataFrame(rowRDD, schema).select("ORIGIN_AIRPORT", "SCHEDULED_DEPARTURE", "TAXI_OUT_TEMP")

      tempFlightsDF
        .withColumn("TIME_SLOT", getTimeSlot(tempFlightsDF("SCHEDULED_DEPARTURE")))
        .withColumn("TAXI_OUT", stringToDouble(tempFlightsDF("TAXI_OUT_TEMP")))
        .select("ORIGIN_AIRPORT", "TIME_SLOT", "TAXI_OUT")
    }


    flightsDF.groupBy("ORIGIN_AIRPORT", "TIME_SLOT").avg("TAXI_OUT")
      .join(airportsDF, flightsDF("ORIGIN_AIRPORT") === airportsDF("IATA_CODE"))
      .select("AIRPORT", "TIME_SLOT", "avg(TAXI_OUT)")
      .repartition(1)
      .sortWithinPartitions($"avg(TAXI_OUT)".desc)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/outputs/spark-sql/airports")

  }
}
