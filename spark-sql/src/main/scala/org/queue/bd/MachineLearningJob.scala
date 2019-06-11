package org.queue.bd

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegression, NaiveBayes}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.TimeSlot

object MachineLearningJob {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL MachineLearningJob - Decision Tree").getOrCreate()
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

    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
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
      .withColumn("AverageAirlineDelay", stringToDouble(airlinesStatistics("STATISTICS_AVG_DELAY")))
      .withColumn("Delay", intToDouble(extractDelayThreeshold(stringToInt(trainData("ARRIVAL_DELAY")))))


    //Airports Statistics
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType,StructField,StringType}
    val airportsStatisticsFile = sc.textFile("hdfs:/user/jgiovanelli/outputs/spark-sql/airports/")
    val schemaString2 = "AIRPORT TIME_SLOT AVG_TAXI_OUT"

    val schema2 = StructType(schemaString2.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD2 = airportsStatisticsFile.map(_.split(",")).map(e => Row(e(0), e(1), e(2)))
    val airportsStatistics = sqlContext.createDataFrame(rowRDD2, schema2)

    val trainData3 = trainData2
      .join(airportsStatistics, trainData2("OriginAirportName") === airportsStatistics("AIRPORT") && trainData2("TimeSlot") === airportsStatistics("TIME_SLOT"))

    val trainDataFinal = trainData3
      .withColumn("AverageTaxiOut", stringToDouble(trainData3("AVG_TAXI_OUT")))


    // Re-balancing (weighting) of records to be used in the logistic loss objective function
    /*val numPositeves = trainData2.filter(trainData2("Delay2") === 1).count
    val datasetSize = trainData2.count
    val balancingRatio = (datasetSize - numPositeves).toDouble / datasetSize

    val calculateWeights = sqlContext.udf.register("sample", (d: Double) => {
      if (d == 1) {
        1 * balancingRatio
      }
      else {
        1 * (1.0 - balancingRatio)
      }
    })*/

   /* val trainDataFinal = trainData3
      .withColumn("Delay", calculateWeights(trainData2("Delay2")))*/

    trainDataFinal.printSchema()
    trainDataFinal.show(5)

    import org.apache.spark.ml.feature.StringIndexer

    val airlineInd = new StringIndexer()
      .setInputCol("Airline")
      .setOutputCol("AirlineIndex")

    val timeSlotInd = new StringIndexer()
      .setInputCol("TimeSlot")
      .setOutputCol("TimeSlotIndex")

    val originStateInd = new StringIndexer()
      .setInputCol("OriginState")
      .setOutputCol("OriginStateIndex")

    val destinationStateInd = new StringIndexer()
      .setInputCol("DestinationState")
      .setOutputCol("DestinationStateIndex")

    val delayInd = new StringIndexer()
      .setInputCol("Delay")
      .setOutputCol("DelayIndex")

    import org.apache.spark.ml.feature.Bucketizer

    val distanceSplits = Range(0, 3000, 200).map(x => x.toDouble).toArray :+ Double.PositiveInfinity

    val distanceBucketize = new Bucketizer()
      .setInputCol("Distance")
      .setOutputCol("DistanceBucketed")
      .setSplits(distanceSplits)

    import org.apache.spark.ml.feature.VectorAssembler

    val assembler = new VectorAssembler()
      .setInputCols(Array("AirlineIndex", "AverageAirlineDelay", "TimeSlotIndex", "Month", "DayOfWeek",
        "DistanceBucketed", "OriginLatitudeArea", "OriginLongitudeArea", "AverageTaxiOut",
        "DestinationLatitudeArea", "DestinationLongitudeArea"))
      .setOutputCol("features")

    /*import org.apache.spark.ml.feature.Normalizer

    val normalizer = new Normalizer()
      .setInputCol("features_temp")
      .setOutputCol("features")*/

    import org.apache.spark.ml.classification.DecisionTreeClassifier


    val dt = new DecisionTreeClassifier()
      .setMaxDepth(10)
      .setMaxBins(20)
      .setFeaturesCol("features")
      .setLabelCol("DelayIndex")

    /*val lr = new LogisticRegression()
      .setMaxIter(10)
      .setFeaturesCol("features")
      .setLabelCol("DelayIndex")*/

    /*val nv = new NaiveBayes()
      .setFeaturesCol("features")
      .setLabelCol("DelayIndex")*/

    import org.apache.spark.ml.Pipeline

    val pipelineDt = new Pipeline()
      .setStages(Array(airlineInd, timeSlotInd, originStateInd, destinationStateInd, delayInd, distanceBucketize, assembler, dt))

    // Training

    import org.apache.spark.mllib.util.MLUtils

    val splits = trainDataFinal.randomSplit(Array(0.7, 0.3), seed = 1L)

    val train = splits(0).cache()
    val test = splits(1).cache()

    val model = pipelineDt.fit(train)

    // Evaluation

    import spark.implicits._

    var trainResult = model.transform(train)

    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

    trainResult = trainResult.select("prediction", "DelayIndex")

    val trainPredictionAndLabels = trainResult
      .map(row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double]))

    val trainMetrics = new BinaryClassificationMetrics(trainPredictionAndLabels.rdd)

    val trainAccuracy = trainResult.filter("prediction=DelayIndex").count.asInstanceOf[Double] / trainResult.count.asInstanceOf[Double]
    //val weightedTrainAccuracy =  1 / (1 + (1 / (numPositeves / datasetSize) - 1) / (1 / balancingRatio - 1) * (1 / trainAccuracy - 1))
    //val weightedTrainAccuracy = balancingRatio.asInstanceOf[Double] * trainAccuracy.asInstanceOf[Double] / ((balancingRatio.asInstanceOf[Double] - 1.0) * trainAccuracy.asInstanceOf[Double] + 1.0)
    println("Accuracy (Train): " + trainAccuracy)

    println("Precision (Train): " +
      trainResult.filter("prediction=1 and DelayIndex=1").count.asInstanceOf[Double] /
        trainResult.filter("prediction=1").count.asInstanceOf[Double])

    println("Recall (Train): " +
      trainResult.filter("prediction=1 and DelayIndex=1").count.asInstanceOf[Double] /
        trainResult.filter("DelayIndex=1").count.asInstanceOf[Double])

    println("Area under ROC curve (Train): " + trainMetrics.areaUnderROC())

    var testResult = model.transform(test)

    testResult = trainResult.select("prediction", "DelayIndex")

    val testPredictionAndLabels = testResult
      .map(row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double]))

    val testMetrics = new BinaryClassificationMetrics(testPredictionAndLabels.rdd)

    val testAccuracy = testResult.filter("prediction=DelayIndex").count.asInstanceOf[Double] / testResult.count.asInstanceOf[Double]
    //val weightedTestAccuracy =  1 / (1 + (1 / (numPositeves / datasetSize) - 1) / (1 / balancingRatio - 1) * (1 / testAccuracy - 1))
    //val weightedTestAccuracy = balancingRatio.asInstanceOf[Double] * testAccuracy.asInstanceOf[Double] / ((balancingRatio.asInstanceOf[Double] - 1.0) * testAccuracy.asInstanceOf[Double] + 1.0)
    println("Accuracy (Test): " + testAccuracy)

    println("Precision (Test): " +
      testResult.filter("prediction=1 and DelayIndex=1").count.asInstanceOf[Double] /
        testResult.filter("prediction=1").count.asInstanceOf[Double])

    println("Recall (Test): " +
      testResult.filter("prediction=1 and DelayIndex=1").count.asInstanceOf[Double] /
        testResult.filter("DelayIndex=1").count.asInstanceOf[Double])

    println("Area under ROC curve (Test): " + testMetrics.areaUnderROC())

    val treeModel = model.stages(7).asInstanceOf[DecisionTreeClassificationModel]
    val debugDecisionTree = treeModel.toDebugString
    println("Learned classification tree model:\n" + debugDecisionTree)
    val modelFile = "hdfs:/user/jgiovanelli/outputs/spark-sql/machine-learning.txt"

    // Hadoop Config is accessible from SparkContext// Hadoop Config is accessible from SparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // Output file can be created from file system.
    val output = fs.create(new Path(modelFile))

    val writer = new PrintWriter(output)
    try {
      writer.write(debugDecisionTree)
    }
    finally {
      writer.close()
    }
  }
}
