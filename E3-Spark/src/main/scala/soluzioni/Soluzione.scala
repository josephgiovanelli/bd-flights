package soluzioni

import org.apache.spark.{SparkConf, SparkContext}

class Soluzione extends App {

  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")
  val rddDC = sc.textFile("hdfs:/bigdata/dataset/divinacommedia")

  // Exercise 0

  rddCapra.count()
  rddCapra.collect()

  val rddCapraWords1 = rddCapra.map( x => x.split(" ") )
  rddCapraWords1.collect()
  val rddCapraWords2 = rddCapra.flatMap( x => x.split(" ") )
  rddCapraWords2.collect()

  val rddDcWords1 = rddDC.map( x => x.split(" ") )
  rddDcWords1.collect()
  val rddDcWords2 = rddDC.flatMap( x => x.split(" ") )
  rddDcWords2.collect()

  // Exercise 1a: word count

  val rddMap = rddCapraWords2.map( x => (x,1))
  rddMap.take(5)

  val rddReduce = rddMap.reduceByKey((x,y) => x + y)
  rddReduce.collect()

  val rddMap = rddDcWords2.map( x => (x,1))
  rddMap.take(5)

  val rddReduce = rddMap.reduceByKey((x,y) => x + y)
  rddReduce.collect()

  // Exercise 1b: word length count

  val rddMap = rddDcWords2.map( x => (x.length,1))
  rddMap.collect()

  val rddReduce = rddMap.reduceByKey((x,y) => x + y)
  rddReduce.collect()

  // Exercise 1c: avg word length by initial

  val rddMap = rddDcWords2.
    filter( _.length>0 ).
    map( x => (x.substring(0,1), x.length))
  rddMap.collect()

  val rddReduce = rddMap.aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1), (a1,a2)=>(a1._1+a2._1,a1._2+a2._2))
  rddReduce.collect()

  val rddFinal = rddReduce.mapValues(v => v._1/v._2)
  rddFinal.collect()

  // Exercise 1d: inverted index

  val rddMap = rddDcWords2.zipWithIndex()
  rddMap.collect()

  val rddGroup = rddMap.groupByKey()
  rddGroup.collect()

  // Sort an RDD by key

//  rdd.sortByKey()

  // Sort an RDD by value

//  rdd.map({case(k,v) => (v,k)}).sortByKey()

  // Visualize the RDD lineage

  val rddL = sc.
    textFile("hdfs:/bigdata/dataset/capra/capra.txt").
    flatMap( x => x.split(" ") ).
    map(x => (x,1)).
    reduceByKey((x,y)=>x+y)
  rddL.toDebugString

  // RDD Partitioning

  val rddW = sc.textFile("hdfs:/bigdata/dataset/weather")
  rddW.getNumPartitions

  val rddWr = sc.textFile("hdfs:/bigdata/dataset/weather-raw")
  rddWr.getNumPartitions

  rddW.count()
  rddWr.count()

  val rddW_c = rddW.cache()
  rddW_c.count()
  rddW_c.count()

  val rddWr_c = rddWr.cache()
  rddWr_c.count()
  rddWr_c.count()

  val rddWr10_c = rddWr_c.coalesce(10).cache()
  rddWr10_c.count()
  rddWr10_c.count()

  // See details in WebUI: http://137.204.72.242:4041
}
