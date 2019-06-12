package org.queue.bd.ml

import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * SparkSQL job to prepare the data only to the Airlines and Airports Jobs (map-reduce, spark and spark-sql implementations)
  */
object MLSamplingJob {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL SampleJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val mlPreprocessingFile = sc.textFile("hdfs:/user/jgiovanelli/outputs/spark-sql/machine-learning/ml-preprocessing/")
    val schemaString = "Airline TimeSlot Month DayOfWeek Distance OriginLatitudeArea OriginLongitudeArea OriginState DestinationLatitudeArea DestinationLongitudeArea DestinationState OriginAirport AverageAirlineDelay Delay AverageTaxiOut"
    val typeMap = Map("Airline" -> StringType, "TimeSlot" -> StringType, "Month" -> IntegerType, "DayOfWeek" -> IntegerType,
      "Distance" -> DoubleType, "OriginLatitudeArea" -> IntegerType, "OriginLongitudeArea" -> IntegerType,
      "OriginState" -> StringType, "DestinationLatitudeArea" -> IntegerType, "DestinationLongitudeArea" -> IntegerType,
      "DestinationState" -> StringType, "OriginAirport" -> StringType,
      "AverageAirlineDelay" -> DoubleType, "Delay" -> DoubleType, "AverageTaxiOut" -> DoubleType)

    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, typeMap(fieldName), nullable = false)))

    val rowRDD = mlPreprocessingFile.map(_.split(",")).map(e => Row(e(0), e(1), e(2).toInt, e(3).toInt, e(4).toDouble,
      e(5).toInt, e(6).toInt, e(7), e(8).toInt, e(9).toInt, e(10), e(11), e(12).toDouble, e(13).toDouble, e(14).toDouble))
    val trainDataFinal = sqlContext.createDataFrame(rowRDD, schema)

    trainDataFinal
      .sample(withReplacement = false, 0.1)
      .repartition(1)
      .write.mode("overwrite").csv("hdfs:/user/jgiovanelli/outputs/spark-sql/machine-learning/ml-sampling/")


  }
}
