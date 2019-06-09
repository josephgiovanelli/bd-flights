package org.queue.bd

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import pojos.{Airline, Flight}

object AirlinesJob {


  case class YAAirline(iata_code: String, airline: String)
  case class YAFlight(airline: String, arrival_delay: Double)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL AirlinesJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sqlContext.setConf("spark.sql.shuffle.partitions", "20")
    import sqlContext.implicits._

    val airlinesDF =  {
      val airlinesFile = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airlines")
      val schemaString = "IATA_CODE AIRLINE"

      val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false)))
      val rowRDD = airlinesFile.map(_.split(",")).map(e => Row(e(0), e(1)))
      sqlContext.createDataFrame(rowRDD, schema)
    }


    val flightsDF =  {
      val flightsFile = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      val schemaString = "AIRLINE ORIGIN_AIRPORT SCHEDULED_DEPARTURE TAXI_OUT ARRIVAL_DELAY_TEMP"

      val stringToDouble = sqlContext.udf.register("stringToDouble", (s: String) =>  s.toDouble )

      val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false)))
      val rowRDD = flightsFile.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4)))
      val tempFlightsDF = sqlContext.createDataFrame(rowRDD, schema).select("AIRLINE", "ARRIVAL_DELAY_TEMP")

      tempFlightsDF
        .withColumn("ARRIVAL_DELAY", stringToDouble(tempFlightsDF("ARRIVAL_DELAY_TEMP")))
        .select("AIRLINE", "ARRIVAL_DELAY")
    }

    airlinesDF.createOrReplaceTempView("airlines")
    flightsDF.createOrReplaceTempView("flights")

    sqlContext.sql(
      """select A.AIRLINE, SF.AVERAGE_DELAY
        |from (select AIRLINE, avg(ARRIVAL_DELAY) as AVERAGE_DELAY
        |      from flights
        |      group by AIRLINE) SF
        |join airlines A on SF.AIRLINE = A.IATA_CODE""".stripMargin)
      .coalesce(1)
      .sortWithinPartitions($"average_delay".desc)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/outputs/spark-sql/airlines")
  }
}
