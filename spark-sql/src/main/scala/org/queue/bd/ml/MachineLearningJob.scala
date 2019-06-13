package org.queue.bd.ml

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegression}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object MachineLearningJob {

  /**
    * Saves the given content in the given path
    * @param content what to save
    * @param path where to save
    * @param sc the spark context
    */
  def save(content: String, path: String)(implicit sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val output = fs.create(new Path(path))

    val writer = new PrintWriter(output)
    try {
      writer.write(content)
    }
    finally {
      writer.close()
    }
  }

  /**
    * Evaluates the given data set and return the result formatted as a string
    * @param set the set to evaluate
    * @param label the label to insert in the result
    * @param spark the spark session
    * @return the result string
    */
  def evaluate(set: DataFrame, label: String)(implicit spark: SparkSession): String = {

    import spark.implicits._

    val result = set.select("prediction", "DelayIndex")

    val predictionAndLabels = result
      .map(row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double]))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels.rdd)

    val accuracy = result.filter("prediction=DelayIndex").count.asInstanceOf[Double] / result.count.asInstanceOf[Double]

    val precision = result.filter("prediction=1 and DelayIndex=1").count.asInstanceOf[Double] /
      result.filter("prediction=1").count.asInstanceOf[Double]

    val recall = result.filter("prediction=1 and DelayIndex=1").count.asInstanceOf[Double] /
      result.filter("DelayIndex=1").count.asInstanceOf[Double]

    val curveROC = metrics.areaUnderROC()

      s"""
         |Accuracy ($label): $accuracy
         |Precision ($label): $precision
         |Recall ($label):  $recall
         |Area under ROC curve ($label): $curveROC
         |""".stripMargin
  }

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder().appName("SparkSQL MachineLearningJob - Logistic Regression").getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext

    //loading MLPreprocessing output
    val mlPreprocessingFile = sc.textFile("hdfs:/user/jgiovanelli/outputs/spark-sql/machine-learning/ml-preprocessing/")
    val schemaString = "Airline TimeSlot Month DayOfWeek Distance OriginLatitudeArea OriginLongitudeArea OriginState DestinationLatitudeArea DestinationLongitudeArea DestinationState OriginAirport AverageArrivalDelay Delay AverageTaxiOut"
    val typeMap = Map("Airline" -> StringType, "TimeSlot" -> StringType, "Month" -> IntegerType, "DayOfWeek" -> IntegerType,
    "Distance" -> DoubleType, "OriginLatitudeArea" -> IntegerType, "OriginLongitudeArea" -> IntegerType,
      "OriginState" -> StringType, "DestinationLatitudeArea" -> IntegerType, "DestinationLongitudeArea" -> IntegerType,
      "DestinationState" -> StringType, "OriginAirport" -> StringType,
      "AverageArrivalDelay" -> DoubleType, "Delay" -> DoubleType, "AverageTaxiOut" -> DoubleType)

    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, typeMap(fieldName), nullable = false)))

    val rowRDD = mlPreprocessingFile.map(_.split(",")).map(e => Row(e(0), e(1), e(2).toInt, e(3).toInt, e(4).toDouble,
      e(5).toInt, e(6).toInt, e(7), e(8).toInt, e(9).toInt, e(10), e(11), e(12).toDouble, e(13).toDouble, e(14).toDouble))
    val trainDataFinal = sqlContext.createDataFrame(rowRDD, schema)

    //creating indexers
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

    val originAirportInd = new StringIndexer()
      .setInputCol("OriginAirport")
      .setOutputCol("OriginAirportIndex")

    val delayInd = new StringIndexer()
      .setInputCol("Delay")
      .setOutputCol("DelayIndex")

    //creating bucketizer
    import org.apache.spark.ml.feature.Bucketizer

    val distanceSplits = Range(0, 3000, 200).map(x => x.toDouble).toArray :+ Double.PositiveInfinity

    val distanceBucketize = new Bucketizer()
      .setInputCol("Distance")
      .setOutputCol("DistanceBucketed")
      .setSplits(distanceSplits)

    val arrivalDelaySplits = Range(-1, 17, 3).map(x => x.toDouble).toArray :+ Double.PositiveInfinity

    val arrivalDelayBucketize = new Bucketizer()
      .setInputCol("AverageArrivalDelay")
      .setOutputCol("AverageArrivalDelayBucketed")
      .setSplits(arrivalDelaySplits)

    val taxiOutSplits = Range(0, 30, 5).map(x => x.toDouble).toArray :+ Double.PositiveInfinity

    val taxiOutBucketize = new Bucketizer()
      .setInputCol("AverageTaxiOut")
      .setOutputCol("AverageTaxiOutBucketed")
      .setSplits(taxiOutSplits)

    import org.apache.spark.ml.feature.VectorAssembler

    //creating assembler
    val assembler = new VectorAssembler()
      .setInputCols(Array("AirlineIndex", "AverageArrivalDelayBucketed", "OriginStateIndex", "DestinationStateIndex",
        "TimeSlotIndex", "Month", "DayOfWeek", "DistanceBucketed", "OriginLatitudeArea", "OriginLongitudeArea",
        "OriginAirportIndex", "AverageTaxiOutBucketed", "DestinationLatitudeArea", "DestinationLongitudeArea"))
      .setOutputCol("features")



    //creating model and relative pipeline
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.classification.DecisionTreeClassifier
    import org.apache.spark.ml.feature.Normalizer


    val pipelineDt = {
      if(args(0) == "tree") {
        val dt = new DecisionTreeClassifier()
          .setMaxDepth(10)
          .setMaxBins(322)
          .setFeaturesCol("features")
          .setLabelCol("DelayIndex")

        new Pipeline()
          .setStages(Array(airlineInd, timeSlotInd, originStateInd, destinationStateInd, originAirportInd, delayInd,
            distanceBucketize, arrivalDelayBucketize, taxiOutBucketize, assembler, dt))
      } else {
        val normalizer = new Normalizer()
          .setInputCol("features")
          .setOutputCol("normalized_features")

        val lr = new LogisticRegression()
          .setMaxIter(10)
          .setFeaturesCol("normalized_features")
          .setLabelCol("DelayIndex")

        new Pipeline()
          .setStages(Array(airlineInd, timeSlotInd, originStateInd, destinationStateInd, originAirportInd, delayInd, distanceBucketize, assembler, normalizer, lr))

      }
    }


    //training

    val splits = trainDataFinal.randomSplit(Array(0.7, 0.3), seed = 1L)

    val train = splits(0).cache()
    val test = splits(1).cache()

    val model = pipelineDt.fit(train)

    //train evaluation

    var trainResult = model.transform(train)

    val trainEvaluation = evaluate(trainResult, "Train")


    //test evaluation
    var testResult = model.transform(test)

    val testEvaluation = evaluate(testResult, "Test")


    //printing the evaluation
    save(trainEvaluation + testEvaluation,
      s"hdfs:/user/jgiovanelli/outputs/spark-sql/machine-learning/ml-results/${args(0)}/model-evaluation.txt")

    //printing tree model
    if (args(0) == "tree") {
      save(model.stages(10).asInstanceOf[DecisionTreeClassificationModel].toDebugString,
        s"hdfs:/user/jgiovanelli/outputs/spark-sql/machine-learning/ml-results/${args(0)}/tree-model.txt")
    }

  }
}
