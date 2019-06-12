package org.queue.bd

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegression, NaiveBayes}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import utils.TimeSlot

object MachineLearningJob {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL MachineLearningJob - Decision Tree").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val mlPreprocessingFile = sc.textFile("hdfs:/user/jgiovanelli/outputs/spark-sql/ml-preprocessing/")
    val schemaString = "Airline TimeSlot Month DayOfWeek Distance OriginLatitudeArea OriginLongitudeArea OriginState DestinationLatitudeArea DestinationLongitudeArea DestinationState OriginAirport AverageAirlineDelay Delay AverageTaxiOut"
    /*val typeMap = Map("Airline" -> StringType, "TimeSlot" -> StringType, "Month" -> IntegerType, "DayOfWeek" -> IntegerType, "Distance" -> DoubleType, "OriginLatitudeArea" -> IntegerType, "OriginLongitudeArea" -> IntegerType,
      "OriginState" -> StringType, "DestinationLatitudeArea" -> IntegerType, "DestinationLongitudeArea" -> IntegerType, "DestinationState" -> StringType, "OriginAirport" -> StringType,
      "AverageAirlineDelay" -> DoubleType, "Delay" -> DoubleType, "AverageTaxiOut" -> DoubleType)*/

    //val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, typeMap(fieldName), nullable = false)))
    val schema = StructType(List(StructField("Airline", StringType, nullable = false)) :+
      StructField("Airline", StringType, nullable = false) :+
      StructField("TimeSlot", StringType, nullable = false) :+
      StructField("Month", StringType, nullable = false) :+
      StructField("DayOfWeek", StringType, nullable = false) :+
      StructField("OriginLatitudeArea", StringType, nullable = false) :+
      StructField("OriginLongitudeArea", StringType, nullable = false) :+
      StructField("OriginState", StringType, nullable = false) :+
      StructField("DestinationLatitudeArea", StringType, nullable = false) :+
      StructField("DestinationLongitudeArea", StringType, nullable = false) :+
      StructField("DestinationState", StringType, nullable = false) :+
      StructField("OriginAirport", StringType, nullable = false) :+
      StructField("AverageAirlineDelay", StringType, nullable = false) :+
      StructField("Delay", StringType, nullable = false) :+
      StructField("AverageTaxiOut", StringType, nullable = false))

    val rowRDD = mlPreprocessingFile.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14)))
    val trainDataFinal = sqlContext.createDataFrame(rowRDD, schema)



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

    val originAirport = new StringIndexer()
      .setInputCol("OriginAirport")
      .setOutputCol("OriginAirportIndex")

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
      .setInputCols(Array("AirlineIndex", "AverageAirlineDelay", "OriginAirportIndex", "TimeSlotIndex", "Month", "DayOfWeek",
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
      .setStages(Array(airlineInd, timeSlotInd, originStateInd, destinationStateInd, originAirport, delayInd, distanceBucketize, assembler, dt))

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
