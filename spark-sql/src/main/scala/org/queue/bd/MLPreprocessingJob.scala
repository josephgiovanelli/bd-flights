package org.queue.bd

import org.apache.spark.sql.SparkSession
import utils.TimeSlot

object MLPreprocessingJob {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL MLPreprocessingJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    //Airports
    val airports = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("hdfs:/user/jgiovanelli/flights-dataset/raw/airports.csv")
      .select("IATA_CODE", "AIRPORT", "LATITUDE", "LONGITUDE", "STATE")

    //Airlines
    val airlines = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("hdfs:/user/jgiovanelli/flights-dataset/raw/airlines.csv")
      .withColumnRenamed("IATA_CODE", "AIRLINES_IATA_CODE")
      .withColumnRenamed("AIRLINE", "AIRLINES_NAME")

    //Flights
    val flights = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("hdfs:/user/jgiovanelli/flights-dataset/raw/flights.csv")
      .filter(x => x.getAs[String]("CANCELLED") == "0" && x.getAs[String]("DIVERTED") == "0" &&
        x.getAs[String]("ORIGIN_AIRPORT").length == 3)
      .select("ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "AIRLINE", "SCHEDULED_DEPARTURE", "MONTH", "DAY_OF_WEEK", "DISTANCE", "ARRIVAL_DELAY")

    //Flights Join Origin Airport
    val trainDataTemp = flights
      .join(airports, flights("ORIGIN_AIRPORT") === airports("IATA_CODE"))
      .withColumnRenamed("IATA_CODE", "ORIGIN_IATA_CODE")
      .withColumnRenamed("AIRPORT", "ORIGIN_AIRPORT_NAME")
      .withColumnRenamed("LATITUDE", "ORIGIN_LATITUDE")
      .withColumnRenamed("LONGITUDE", "ORIGIN_LONGITUDE")
      .withColumnRenamed("STATE", "ORIGIN_STATE")

    //Flights Join Destination Airport
    val trainDataTemp2 = trainDataTemp
      .join(airports, trainDataTemp("DESTINATION_AIRPORT") === airports("IATA_CODE"))
      .withColumnRenamed("IATA_CODE", "DESTINATION_IATA_CODE")
      .withColumnRenamed("AIRPORT", "DESTINATION_AIRPORT_NAME")
      .withColumnRenamed("LATITUDE", "DESTINATION_LATITUDE")
      .withColumnRenamed("LONGITUDE", "DESTINATION_LONGITUDE")
      .withColumnRenamed("STATE", "DESTINATION_STATE")

    //Flights Join Airline
    val trainDataTemp3 = trainDataTemp2
      .join(airlines, trainDataTemp("AIRLINE") === airlines("AIRLINES_IATA_CODE"))

    //Flights Join Airlines Statistics
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType,StructField,StringType}
    val airlinesStatisticsFile = sc.textFile("hdfs:/user/jgiovanelli/outputs/spark-sql/airlines/")
    val schemaString = "STATISTIC_AIRLINES_NAME STATISTICS_AVG_DELAY"

    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false)))
    val rowRDD = airlinesStatisticsFile.map(_.split(",")).map(e => Row(e(0), e(1)))
    val airlinesStatistics = sqlContext.createDataFrame(rowRDD, schema)

    val trainData = trainDataTemp3
      .join(airlinesStatistics, trainDataTemp3("AIRLINES_NAME") === airlinesStatistics("STATISTIC_AIRLINES_NAME"))
      .cache()

    trainData.printSchema()
    trainData.show(5)
    trainData.describe().show()

    // Pre processing

    val extractTimeSlot = sqlContext.udf.register("extractTimeSlot",
      (scheduled_departure: String) => {
        TimeSlot.getTimeSlot(scheduled_departure).getDescription
      })

    val extractDelayThreeshold = sqlContext.udf.register("extractDelayThreeshold",
      (arrival_delay: Int) => {
        if (arrival_delay > 0) {
          "1"
        } else {
          "0"
        }
      })

    val extractLatitudeArea = sqlContext.udf.register("extractLatitudeArea",
      (latitude: Double) => {
        if (latitude < 30.0) {
          "0"
        } else if (latitude < 40.0){
          "1"
        } else if (latitude < 50.0){
          "2"
        } else {
          "3"
        }
      })

    val extractLongitudeArea = sqlContext.udf.register("extractLongitudeArea",
      (longitude: Double) => {
        if (longitude < -130.0) {
          "0"
        } else if (longitude < -110.0){
          "1"
        } else if (longitude < -90.0){
          "2"
        } else {
          "3"
        }
      })


    val stringToInt = sqlContext.udf.register("stringToInt", (s: String) => s.toInt)
    val intToDouble = sqlContext.udf.register("intToDouble", (i: Int) => i.toDouble)
    val stringToDouble = sqlContext.udf.register("stringToDouble", (s: String) =>  {
      val value = try { Some(s.toDouble) } catch { case _ => None }
      value match {
        case Some(e) => e
        case _ => -80.0
      }
    })


    val trainData2 = trainData
      .withColumn("Airline", trainData("AIRLINE"))
      .withColumn("TimeSlot", extractTimeSlot(trainData("SCHEDULED_DEPARTURE")))
      .withColumn("Month", stringToInt(trainData("MONTH")))
      .withColumn("DayOfWeek", stringToInt(trainData("DAY_OF_WEEK")))
      .withColumn("Distance", intToDouble(trainData("DISTANCE")))
      .withColumn("OriginLatitudeArea", stringToInt(extractLatitudeArea(stringToDouble(trainData("ORIGIN_LATITUDE")))))
      .withColumn("OriginLongitudeArea", stringToInt(extractLongitudeArea(stringToDouble(trainData("ORIGIN_LONGITUDE")))))
      .withColumn("OriginState", trainData("ORIGIN_STATE"))
      .withColumn("DestinationLatitudeArea", stringToInt(extractLatitudeArea(stringToDouble(trainData("DESTINATION_LATITUDE")))))
      .withColumn("DestinationLongitudeArea", stringToInt(extractLongitudeArea(stringToDouble(trainData("DESTINATION_LONGITUDE")))))
      .withColumn("DestinationState", trainData("DESTINATION_STATE"))
      .withColumn("OriginAirportName", trainData("ORIGIN_AIRPORT_NAME"))
      .withColumn("OriginAirport", trainData("ORIGIN_AIRPORT"))
      .withColumn("AverageAirlineDelay", stringToDouble(airlinesStatistics("STATISTICS_AVG_DELAY")))
      .withColumn("Delay", intToDouble(extractDelayThreeshold(stringToInt(trainData("ARRIVAL_DELAY")))))


    //Airports Statistics
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType,StructField,StringType}
    val airportsStatisticsFile = sc.textFile("hdfs:/user/jgiovanelli/outputs/spark-sql/airports/")
    val schemaString2 = "AIRPORT TIME_SLOT AVG_TAXI_OUT"

    val schema2 = StructType(schemaString2.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = false)))
    val rowRDD2 = airportsStatisticsFile.map(_.split(",")).map(e => Row(e(0), e(1), e(2)))
    val airportsStatistics = sqlContext.createDataFrame(rowRDD2, schema2)

    val trainData3 = trainData2
      .join(airportsStatistics, trainData2("OriginAirportName") === airportsStatistics("AIRPORT") && trainData2("TimeSlot") === airportsStatistics("TIME_SLOT"))

    val trainDataFinal = trainData3
      .withColumn("AverageTaxiOut", stringToDouble(trainData3("AVG_TAXI_OUT")))
      .select("Airline", "TimeSlot", "Month", "DayOfWeek", "Distance", "OriginLatitudeArea", "OriginLongitudeArea",
        "OriginState", "DestinationLatitudeArea", "DestinationLongitudeArea", "DestinationState", "OriginAirport",
        "AverageAirlineDelay", "Delay", "AverageTaxiOut")
      .repartition(1)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/outputs/spark-sql/ml-preprocessing")




  }
}
