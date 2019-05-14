class Esercizi {

  ////////// Exercise 0: setup
  // Only once

  import org.apache.spark.streaming._
  import org.apache.spark.storage.StorageLevel

  // Remember to check the IP address!!

  val ssc = new StreamingContext(sc, Seconds(3))
  val lines = ssc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER)
  val words = lines.flatMap(_.split(" "))
  val count = words.count()
  count.print()
  ssc.start()

  // Copy/paste this final command to terminate the application

  ssc.stop(false)



  ////////// Exercise 1: word count

  import org.apache.spark.streaming._
  import org.apache.spark.storage.StorageLevel

  val ssc = new StreamingContext(sc, Seconds(3))
  val lines = ssc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) })
  wordCounts.print()
  ssc.start()

  ssc.stop(false)



  ////////// Exercise 2: checkpointing

  import org.apache.spark.streaming._
  import org.apache.spark.storage.StorageLevel

  def functionToCreateContext(): StreamingContext = {
    val newSsc = new StreamingContext(sc, Seconds(3))
    val lines = newSsc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) })
    wordCounts.print()
    newSsc.checkpoint("hdfs:/user/egallinucci/streaming/checkpoint")
    newSsc
  }

  val ssc = StreamingContext.getOrCreate("hdfs:/user/egallinucci/streaming/checkpoint", functionToCreateContext _)
  ssc.start()

  ssc.stop(false)



  ////////// Exercise 3: stateful operations

  import org.apache.spark.streaming._
  import org.apache.spark.storage.StorageLevel

  def updateFunction( newValues: Seq[Int], oldValue: Option[Int] ): Option[Int] = {
    Some(oldValue.getOrElse(0) + newValues.sum)
  }

  def functionToCreateContext(): StreamingContext = {
    val newSsc = new StreamingContext(sc, Seconds(3))
    val lines = newSsc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val cumulativeWordCounts = words.map(x => (x, 1)).updateStateByKey(updateFunction)
    cumulativeWordCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()
    newSsc.checkpoint("hdfs:/user/egallinucci/streaming/checkpoint2")
    newSsc
  }

  val ssc = StreamingContext.getOrCreate("hdfs:/user/egallinucci/streaming/checkpoint2", functionToCreateContext _)
  ssc.start()

  ssc.stop(false)



  ////////// Exercise 4: sliding, overlapping windows

  import org.apache.spark.streaming._
  import org.apache.spark.storage.StorageLevel

  val ssc = new StreamingContext(sc, Seconds(3))
  val lines = ssc.socketTextStream("137.204.72.242",9999,StorageLevel.MEMORY_AND_DISK_SER).window(Seconds(30), Seconds(3))
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
  wordCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()

  ssc.start()

  ssc.stop(false)



  ////////// Exercise 5: trending hashtags in the last minute

  import org.apache.spark.streaming._
  import org.apache.spark.storage.StorageLevel

  val ssc = new StreamingContext(sc, Seconds(10))
  val lines = ssc.socketTextStream("137.204.72.240",8765,StorageLevel.MEMORY_AND_DISK_SER).window(Seconds(60), Seconds(10))
  val hashtag = lines.filter(_.nonEmpty).map( _.split("\\|") ).map(_(2)).filter(_.nonEmpty)
  val hashtagCounts = hashtag.map(x => (x, 1)).reduceByKey(_ + _)
  hashtagCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()

  ssc.start()

  ssc.stop(false)



  ////////// Exercise 6: cumulative number of tweets by city

  import org.apache.spark.streaming._
  import org.apache.spark.storage.StorageLevel

  def updateFunction( newValues: Seq[Int], oldValue: Option[Int] ): Option[Int] = {
    Some(oldValue.getOrElse(0) + newValues.sum)
  }

  def functionToCreateContext(): StreamingContext = {
    val newSsc = new StreamingContext(sc, Seconds(3))
    val lines = newSsc.socketTextStream("137.204.72.240",8765,StorageLevel.MEMORY_AND_DISK_SER)
    val hashtag = lines.filter(_.nonEmpty).map( _.split("\\|") ).map(_(4)).filter(_.nonEmpty)
    val hashtagCounts = hashtag.map(x => (x, 1)).updateStateByKey(updateFunction)
    hashtagCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()
    newSsc.checkpoint("hdfs:/user/epierfederici/streaming/checkpoint6")
    newSsc
  }

  val ssc = StreamingContext.getOrCreate("hdfs:/user/epierfederici/streaming/checkpoint6", functionToCreateContext _)
  ssc.start()

  ssc.stop(false)



  ////////// Exercise 7: cumulative number of tweets by country and also add the average sentiment

  def updateFunction( newValues: Seq[(Int,Int)], oldValue: Option[(Int,Int,Int,Double)] ): Option[(Int,Int,Int,Double)] = {
    oldValue.getOrElse(0,0,0,0.0) // to initialize oldValue
    val totSentiment = newValues.map(_._2).sum // to sum the first elements of newValues
    val totTweets =  newValues.map(_._1).sum
    val countSentiment =  newValues.filter(_._2 != 0).map(_._1).sum
    val avgSentiment = totSentiment / countSentiment
    Some((totTweets, totSentiment, countSentiment, avgSentiment))
  }

  def functionToCreateContext(): StreamingContext = {
    val newSsc = new StreamingContext(sc, Seconds(3))
    val lines = newSsc.socketTextStream("137.204.72.240",8765,StorageLevel.MEMORY_AND_DISK_SER)
    val cities = lines.filter(_.nonEmpty).map( _.split("\\|") ).map(x => (x(7), (1, x(3).toInt)))
    val citiesCounts = cities.map(x => (x, 1)).updateStateByKey(updateFunction)
    citiesCounts.map({case(k,v)=>(v,k)}).transform({ rdd => rdd.sortByKey(false) }).print()
    newSsc.checkpoint("hdfs:/user/epierfederici/streaming/checkpoint7")
    newSsc
  }

  val ssc = StreamingContext.getOrCreate("hdfs:/user/epierfederici/streaming/checkpoint7", functionToCreateContext _)
  ssc.start()

  ssc.stop(false)
}
