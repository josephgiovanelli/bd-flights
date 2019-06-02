package org.queue.bd

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegression}
import org.apache.spark.sql.SparkSession
import utils.TimeSlot

object MachineLearningJob {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL PreprocessingJob").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    //Flights
    val trainData = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("hdfs:/user/jgiovanelli/flights-dataset/raw/flights.csv")
      .filter(x => x.getAs[String]("CANCELLED") == "0" && x.getAs[String]("DIVERTED") == "0" &&
        x.getAs[String]("ORIGIN_AIRPORT").length == 3)
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
        if (arrival_delay > 10) {
          "1"
        } else {
          "0"
        }
      })

    val toInt = sqlContext.udf.register("toInt", (s: String) => s.toInt)
    val toDouble = sqlContext.udf.register("toDouble", (i: Int) => i.toDouble)


    val trainDataFinal = trainData
      .withColumn("Airline", trainData("AIRLINE"))
      .withColumn("Airport", trainData("ORIGIN_AIRPORT"))
      .withColumn("TimeSlot", extractTimeSlot(trainData("SCHEDULED_DEPARTURE")))
      .withColumn("Delay", toDouble(extractDelayThreeshold(toInt(trainData("ARRIVAL_DELAY")))))

    import org.apache.spark.ml.feature.StringIndexer

    val airlineInd = new StringIndexer()
      .setInputCol("Airline")
      .setOutputCol("AirlineIndex")

    val airportInd = new StringIndexer()
      .setInputCol("Airport")
      .setOutputCol("AirportIndex")

    val timeSlotInd = new StringIndexer()
      .setInputCol("TimeSlot")
      .setOutputCol("TimeSlotIndex")

    val delayInd = new StringIndexer()
      .setInputCol("Delay")
      .setOutputCol("DelayIndex")

    import org.apache.spark.ml.feature.VectorAssembler

    val assembler = new VectorAssembler()
      .setInputCols(Array("AirlineIndex", "AirportIndex", "TimeSlotIndex"))
      .setOutputCol("features")

    import org.apache.spark.ml.classification.DecisionTreeClassifier


    val dt = new DecisionTreeClassifier()
      .setMaxDepth(5)
      .setMaxBins(350)

    val lr = new LogisticRegression().setMaxIter(10)

    dt.setFeaturesCol("features")
      .setLabelCol("DelayIndex")

    lr.setFeaturesCol("features")
      .setLabelCol("DelayIndex")

    import org.apache.spark.ml.Pipeline

    val pipelineDt = new Pipeline()
      //.setStages(Array(airlineInd, airportInd, timeSlotInd, delayInd, assembler, dt))
      .setStages(Array(airlineInd, airportInd, timeSlotInd, delayInd, assembler, lr))

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

    /*val treeModel = model.stages(5).asInstanceOf[DecisionTreeClassificationModel]
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
    }*/
  }
}
