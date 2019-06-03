package org.queue.bd

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegression}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.TimeSlot

object MachineLearningJob {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL PreprocessingJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    //Airports
    val airports = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("hdfs:/user/jgiovanelli/flights-dataset/raw/airports.csv")
      .select("IATA_CODE", "LATITUDE", "LONGITUDE", "STATE")

    //Flights
    val flights = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("hdfs:/user/jgiovanelli/flights-dataset/raw/flights.csv")
      .filter(x => x.getAs[String]("CANCELLED") == "0" && x.getAs[String]("DIVERTED") == "0" &&
        x.getAs[String]("ORIGIN_AIRPORT").length == 3)
      .select("ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "AIRLINE", "SCHEDULED_DEPARTURE", "MONTH", "DISTANCE", "ARRIVAL_DELAY")
      .cache()

    val trainDataTemp = flights
      .join(airports, flights("ORIGIN_AIRPORT") === airports("IATA_CODE"))
      .withColumnRenamed("IATA_CODE", "ORIGIN_IATA_CODE")
      .withColumnRenamed("LATITUDE", "ORIGIN_LATITUDE")
      .withColumnRenamed("LONGITUDE", "ORIGIN_LONGITUDE")
      .withColumnRenamed("STATE", "ORIGIN_STATE")

    val trainData = trainDataTemp
      .join(airports, trainDataTemp("DESTINATION_AIRPORT") === airports("IATA_CODE"))
      .withColumnRenamed("IATA_CODE", "DESTINATION_IATA_CODE")
      .withColumnRenamed("LATITUDE", "DESTINATION_LATITUDE")
      .withColumnRenamed("LONGITUDE", "DESTINATION_LONGITUDE")
      .withColumnRenamed("STATE", "DESTINATION_STATE")
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
        if (latitude < 30) {
          "0"
        } else if (latitude < 40){
          "1"
        } else if (latitude < 50){
          "2"
        } else {
          "3"
        }
      })

    val extractLongitudeArea = sqlContext.udf.register("extractLongitudeArea",
      (longitude: Double) => {
        if (longitude < -130) {
          "0"
        } else if (longitude < -110){
          "1"
        } else if (longitude < -90){
          "2"
        } else {
          "3"
        }
      })


    val stringToInt = sqlContext.udf.register("stringToInt", (s: String) => s.toInt)
    val intToDouble = sqlContext.udf.register("intToDouble", (i: Int) => i.toDouble)
    val stringToDouble = sqlContext.udf.register("stringToDouble", (s: String) => s.toDouble)

    val trainData2 = trainData
      .withColumn("Airline", trainData("AIRLINE"))
      .withColumn("TimeSlot", extractTimeSlot(trainData("SCHEDULED_DEPARTURE")))
      .withColumn("Month", stringToInt(trainData("MONTH")))
      .withColumn("Distance", intToDouble(trainData("DISTANCE")))
      .withColumn("OriginLatitudeArea", stringToInt(extractLatitudeArea(stringToDouble(trainData("ORIGIN_LATITUDE")))))
      .withColumn("OriginLongitudeArea", stringToInt(extractLongitudeArea(stringToDouble(trainData("ORIGIN_LONGITUDE")))))
      .withColumn("OriginState", trainData("ORIGIN_STATE"))
      .withColumn("DestinationLatitudeArea", stringToInt(extractLatitudeArea(stringToDouble(trainData("DESTINATION_LATITUDE")))))
      .withColumn("DestinationLongitudeArea", stringToInt(extractLongitudeArea(stringToDouble(trainData("DESTINATION_LONGITUDE")))))
      .withColumn("DestinationState", trainData("DESTINATION_STATE"))
      .withColumn("Delay2", intToDouble(extractDelayThreeshold(stringToInt(trainData("ARRIVAL_DELAY")))))

    def balanceDataset(dataset: DataFrame): DataFrame = {

      // Re-balancing (weighting) of records to be used in the logistic loss objective function
      val numNegatives = dataset.filter(dataset("Delay2") === 0).count
      val datasetSize = dataset.count
      val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize

      val calculateWeights = sqlContext.udf.register("sample", (d: Double) => {
        if (d == 0.0) {
          1 * balancingRatio
        }
        else {
          1 * (1.0 - balancingRatio)
        }
      })

      val weightedDataset = dataset
        .withColumn("Delay", calculateWeights(dataset("Delay2")))
      weightedDataset
    }

    val trainDataFinal = balanceDataset(trainData2)

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
      .setInputCols(Array("AirlineIndex", "TimeSlotIndex", "Month", "DistanceBucketed", "OriginLatitudeArea",
        "OriginLongitudeArea", "OriginStateIndex", "DestinationLatitudeArea", "DestinationLongitudeArea","DestinationStateIndex"))
      .setOutputCol("features")

    import org.apache.spark.ml.classification.DecisionTreeClassifier


    val dt = new DecisionTreeClassifier()
      .setMaxDepth(5)
      .setMaxBins(350)

    //val lr = new LogisticRegression().setMaxIter(10)

    dt.setFeaturesCol("features")
      .setLabelCol("DelayIndex")

    //lr.setFeaturesCol("features")
    //  .setLabelCol("DelayIndex")

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

    println("Accuracy (Train): " +
      trainResult.filter("prediction=DelayIndex").count.asInstanceOf[Double] /
        trainResult.count.asInstanceOf[Double])

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

    println("Accuracy (Test): " +
      testResult.filter("prediction=DelayIndex").count.asInstanceOf[Double] /
        testResult.count.asInstanceOf[Double])

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
