package org.queue.bd

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * SparkSQL job to list the airlines ordered descendingly on the average arrival delay.
  */
object AirlinesJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL AirlinesJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    //loading the airlines data set from the hdfs
    val airlinesDF =  {
      val airlinesFile = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/airlines")
      val schemaString = "IATA_CODE AIRLINE"

      val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false)))
      val rowRDD = airlinesFile.map(_.split(",")).map(e => Row(e(0), e(1)))
      sqlContext.createDataFrame(rowRDD, schema)
    }

    //loading the flights data set from the hdfs
    val flightsDF =  {
      val flightsFile = sc.textFile("hdfs:/user/jgiovanelli/flights-dataset/clean/flights")
      val schemaString = "AIRLINE ORIGIN_AIRPORT SCHEDULED_DEPARTURE TAXI_OUT ARRIVAL_DELAY_TEMP"

      val stringToDouble = sqlContext.udf.register("stringToDouble", (s: String) =>  s.toDouble )

      val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false)))
      val rowRDD = flightsFile.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4)))
      val tempFlightsDF = sqlContext.createDataFrame(rowRDD, schema).select("AIRLINE", "ARRIVAL_DELAY_TEMP")

      //selecting the fields of interest
      tempFlightsDF
        .withColumn("ARRIVAL_DELAY", stringToDouble(tempFlightsDF("ARRIVAL_DELAY_TEMP")))
        .select("AIRLINE", "ARRIVAL_DELAY")
    }

    //defining the temp views on which issue the query
    airlinesDF.createOrReplaceTempView("airlines")
    flightsDF.createOrReplaceTempView("flights")

    //issuing the query
    sqlContext.sql(
      """select A.AIRLINE, SF.AVERAGE_DELAY
        |from (select AIRLINE, avg(ARRIVAL_DELAY) as AVERAGE_DELAY
        |      from flights
        |      group by AIRLINE) SF
        |join airlines A on SF.AIRLINE = A.IATA_CODE""".stripMargin)
      .coalesce(1)
      //sorting the results
      .sortWithinPartitions($"average_delay".desc)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/outputs/spark-sql/airlines")
  }
}
