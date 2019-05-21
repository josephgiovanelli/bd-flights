
class prova {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf

  val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

  // Init SQLContext
  //  val sqlCtx = new org.apache.sql.SQLContext(sc)
  val sqlCtx = new org.apache.spark.sql.SQLContext(sc)

  /*
 * --------------------------------
 * Loading data
 * --------------------------------
 */
  // ------- HIVE -------
  import org.apache.spark.sql.hive.HiveContext

  val hiveContext = new HiveContext(sc)

  //==================================================================
  //Exercise 1

  //Import data from JSON file to DataFrame
  val df_movies = sqlContext.jsonFile("/user/amordenti/movies/movies.json")
  val df_movies = sc.read.format("json").load("/user/amordenti/movies/movies.json")

  //Load to df a CSV without a defined schema
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType, StructField, StringType}

  val population = sc.textFile("population/zipcode_population.csv")
  val schemaString = "zipcode total_population avg_age male female"

  val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
  val rowRDD = population.map(_.split(";")).map(e => Row(e(0), e(1), e(2), e(3), e(4)))
  val peopleDF = sqlContext.createDataFrame(rowRDD, schema)

  //spark approach
  val transaction_RDD = sc.textFile("real_estate/real_estate_transactions.txt")
  val schema_array = transaction_RDD.take(1)
  val schema_string = schema_array(0)
  val schema = StructType(schema_string.split(';').map(fieldName ⇒ StructField(fieldName, StringType, true)))

  val rowRDD = transaction_RDD.map(_.split(";")).map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4)))
  val transaction_DF_tmp = sqlContext.createDataFrame(rowRDD, schema)
  //then remove the first row (which was the schema)
  val transaction_DF = transaction_DF.where("street <> 'street'")

  //spark2 approach
  val df = sqlContext.read.format("csv").option("header", "true").option("delimiter", ";").load("real_estate/real_estate_transactions.txt")


  //ADVANCED - Load to DF a text file with an already existing schema
  import sqlContext.implicits._

  case class Transaction(street: String, city: String, zip: String, state: String, beds: String, baths: String, sq__ft: String, tipo: String, price: String)

  val transaction_df = sc.textFile("real_estate/real_estate_transactions.txt").map(_.split(";")).map(p => Transaction(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8))).toDF()

  //Load user data from parquet file
  val parquet_df = sqlContext.read.load("userdata/userdata.parquet")


  //==================================================================

  //Exercise 2

  //Dataframe to JSON
  peopleDF.write.mode("append").json("people.json")

  //Parquet to Hive Table
  parquet_df.saveAsTable("parquet_table")

  //Movies to Parquet
  df_movies.write.parquet("movies.parquet")

  //==================================================================
  //Exercise 3


  val url = "jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF"
  val table = "US_GEOGRAPHY"

  val geoDF = sqlContext.read.format("jdbc").options(Map("driver" -> "oracle.jdbc.driver.OracleDriver", "url" -> url, "dbtable" -> table)).load()

  val table = "ZIPCODE_POPULATION"

  val popDF = sqlContext.read.format("jdbc").options(Map("url" -> url, "dbtable" -> table)).load()


  geoDF.printSchema()
  // Result
  //root
  // |-- STATE_FIPS: decimal(38,0) (nullable = true)
  // |-- STATE: string (nullable = true)
  // |-- STATE_ABBR: string (nullable = true)
  // |-- ZIPCODE: string (nullable = true)
  // |-- COUNTY: string (nullable = true)
  // |-- CITY: string (nullable = true)

  popDF.printSchema()
  //root
  // |-- ZIPCODE: decimal(8,0) (nullable = true)
  // |-- TOTALPOPULATION: decimal(15,0) (nullable = true)
  // |-- MEDIANAGE: decimal(4,0) (nullable = true)
  // |-- TOTALMALES: integer (nullable = true)
  // |-- TOTALFEMALES: integer (nullable = true)


  geoDF.show()
  popDF.show()


  geoDF.registerTempTable("geo")
  val geoCalifornia = sqlContext.sql("select * from geo where STATE = 'California'")
  geoCalifornia.show()

  //result
  //18/04/14 10:23:23 WARN hdfs.DFSClient: Slow ReadProcessor read fields took 30001ms (threshold=30000ms); ack: seqno: 16 reply: 0 reply: 0 reply: 0 downstreamAckTimeNanos: 1670630, targets: [DatanodeInfoWithStorage[137.204.72.233:50010,DS-30de5ca3-f69f-4d2b-bbac-efbf284650d4,DISK], DatanodeInfoWithStorage[137.204.72.242:50010,DS-11feecaa-91a0-4eea-960e-f318790e565b,DISK], DatanodeInfoWithStorage[137.204.72.236:50010,DS-27736cd3-c9b7-4aa0-9605-b060ccac0a0b,DISK]]
  //+----------+----------+----------+-------+-----------+-----------+
  //|STATE_FIPS|     STATE|STATE_ABBR|ZIPCODE|     COUNTY|       CITY|
  //+----------+----------+----------+-------+-----------+-----------+
  //|         6|California|        CA|  89439|     Sierra|       null|
  //|         6|California|        CA|  90001|Los Angeles|Los angeles|
  //|         6|California|        CA|  90002|Los Angeles|Los angeles|
  //|         6|California|        CA|  90003|Los Angeles|Los angeles|
  //|         6|California|        CA|  90004|Los Angeles|Los angeles|
  //|         6|California|        CA|  90005|Los Angeles|Los angeles|
  //|         6|California|        CA|  90006|Los Angeles|Los angeles|
  //|         6|California|        CA|  90007|Los Angeles|Los angeles|
  //|         6|California|        CA|  90008|Los Angeles|Los angeles|
  //|         6|California|        CA|  90010|Los Angeles|Los angeles|
  //|         6|California|        CA|  90011|Los Angeles|Los angeles|
  //|         6|California|        CA|  90012|Los Angeles|Los angeles|
  //|         6|California|        CA|  90013|Los Angeles|Los angeles|
  //|         6|California|        CA|  90014|Los Angeles|Los angeles|
  //|         6|California|        CA|  90015|Los Angeles|Los angeles|
  //|         6|California|        CA|  90016|Los Angeles|Los angeles|
  //|         6|California|        CA|  90017|Los Angeles|Los angeles|
  //|         6|California|        CA|  90018|Los Angeles|Los angeles|
  //|         6|California|        CA|  90019|Los Angeles|Los angeles|
  //|         6|California|        CA|  90020|Los Angeles|Los angeles|
  //+----------+----------+----------+-------+-----------+-----------+


  popDF.show(100)

  //==================================================================
  //Exercise 4

  popDF.registerTempTable("population")

  val newPopDF = sqlContext.sql("select zip_code, totalpopulation, medianage , round(totalpopulation/1000,1) as totpopulation_k from population")

  //result
  //newPopDF: org.apache.spark.sql.DataFrame = [zipcode: decimal(8,0), totalpopulation: decimal(15,0), medianage: decimal(4,0), totpopulation_k: decimal(26,1)]

  newPopDF.show()

  //Result
  //+-------+---------------+---------+---------------+
  //|zipcode|totalpopulation|medianage|totpopulation_k|
  //+-------+---------------+---------+---------------+
  //|  91371|              1|      735|            0.0|
  //|  90001|          57110|      266|           57.1|
  //|  90002|          51223|      255|           51.2|
  //|  90003|          66266|      263|           66.3|
  //|  90004|          62180|      348|           62.2|
  //|  90005|          37681|      339|           37.7|
  //|  90006|          59185|      324|           59.2|
  //|  90007|          40920|       24|           40.9|
  //|  90008|          32327|      397|           32.3|
  //|  90010|           3800|      378|            3.8|
  //|  90011|         103892|      262|          103.9|
  //|  90012|          31103|      363|           31.1|
  //|  90013|          11772|      446|           11.8|
  //|  90014|           7005|      448|            7.0|
  //|  90015|          18986|      313|           19.0|
  //|  90016|          47596|      339|           47.6|
  //|  90017|          23768|      294|           23.8|
  //|  90018|          49310|      332|           49.3|
  //|  90019|          64458|      358|           64.5|
  //|  90020|          38967|      346|           39.0|
  //+-------+---------------+---------+---------------+
  //only showing top 20 rows


  val joinDF = sqlContext.sql("select STATE, county, geo.ZIPCODE,city,totalpopulation from geo join population on geo.ZIPCODE = population.zip_code where COUNTY='Los Angeles'")

  joinDF.registerTempTable("joinDF")

  val groupDF = sqlContext.sql("select zipcode,round(sum(totalpopulation)/1000,1) as zipcode_population_k from joinDF group by zipcode")

  groupDF.registerTempTable("groupDF")

  val sortedDF = sqlContext.sql("select zipcode,zipcode_population_k from groupDF order by zipcode desc")


  //==================================================================
  //Exercise 5

  val joinedRDD = geoDF.filter("county = 'Los Angeles'").join(popDF, geoDF("ZIPCODE") === popDF("zip_code"))
  val aggrRDD = joinedRDD.groupBy("zip_code").agg(sum("totalpopulation") / 1000)
  val sortedRDD = aggrRDD.orderBy(desc("zip_code"))

  sortedRDD.take(100).foreach(println)

  //Result
  //[Stage 10:=====================================>                (140 + 1) / 200]19/03/17 10:34:40 WARN nio.NioEventLoop: Selector.select() returned prematurely 512 times in a row; rebuilding selector.
  //[93591,7.28500000000]
  //[93563,0.38800000000]
  //[93553,2.13800000000]
  //[93552,38.15800000000]
  //[93551,50.79800000000]
  //[93550,74.92900000000]
  //[93544,1.25900000000]
  //[93543,13.03300000000]
  //[93536,70.91800000000]
  //..
  //..

  //==================================================================
  //Exercise 6

  import sqlContext.implicits._

  case class Transaction(street: String, city: String, zip: String, state: String, beds: String, baths: String, sq__ft: String, tipo: String, price: String)

  val transaction_df = sc.textFile("real_estate/real_estate_transactions.txt").map(_.split(";")).map(p => Transaction(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8))).toDF()

  transaction_df.registerTempTable("transaction")

  val convertedTransactions_df = sqlContext.sql("select city,(price*1.237) price_eur from transaction")

  convertedTransactions_df.registerTempTable("converted")

  val city_avg = sqlContext.sql("select city,avg(price_eur) avg_price from converted group by city")

  city_avg.count()

  ==================================================================
  Exercise
  7


  // Execution plan of Ex5
  sortedRDD.explain
  //== Physical Plan ==
  //Sort [zip_code#6 DESC], true, 0
  //+- ConvertToUnsafe
  //   +- Exchange rangepartitioning(zip_code#6 DESC,200), None
  //      +- ConvertToSafe
  //         +- TungstenAggregate(key=[zip_code#6], functions=[(sum(totalpopulation#7),mode=Final,isDistinct=false)], output=[zip_code#6,((sum(totalpopulation),mode=Complete,isDistinct=false) / 1000)#11])
  //            +- TungstenExchange hashpartitioning(zip_code#6,200), None
  //               +- TungstenAggregate(key=[zip_code#6], functions=[(sum(totalpopulation#7),mode=Partial,isDistinct=false)], output=[zip_code#6,sum#14])
  //                  +- Project [zip_code#6,totalpopulation#7]
  //                     +- SortMergeJoin [cast(ZIPCODE#3 as double)], [cast(zip_code#6 as double)]
  //                        :- Sort [cast(ZIPCODE#3 as double) ASC], false, 0
  //                        :  +- TungstenExchange hashpartitioning(cast(ZIPCODE#3 as double),200), None
  //                        :     +- Project [ZIPCODE#3]
  //                        :        +- Filter (COUNTY#4 = Los Angeles)
  //                        :           +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,US_GEOGRAPHY,[Lorg.apache.spark.Partition;@5bc637fc,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=US_GEOGRAPHY, driver=oracle.jdbc.driver.OracleDriver})[ZIPCODE#3,COUNTY#4] PushedFilters: [EqualTo(COUNTY,Los Angeles)]
  //                        +- Sort [cast(zip_code#6 as double) ASC], false, 0
  //                           +- TungstenExchange hashpartitioning(cast(zip_code#6 as double),200), None
  //                              +- ConvertToUnsafe
  //                                 +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,ZIPCODE_POPULATION,[Lorg.apache.spark.Partition;@1ddeb95,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=ZIPCODE_POPULATION})[zip_code#6,totalpopulation#7]

  // Execution plan of Ex6


  //Immagine WebUI

  //OPTIONAL

  //Standard Join
  val joinedRDD = geoDF.filter("county = 'Los Angeles'").join(popDF, geoDF("ZIPCODE") === popDF("zip_code"))

  //joinedRDD: org.apache.spark.sql.DataFrame = [STATE_FIPS: decimal(38,0), STATE: string, STATE_ABBR: string, ZIPCODE: string, COUNTY: string, CITY: string, ZIP_CODE: decimal(8,0), TOTALPOPULATION: decimal(15,0), MEDIANAGE: decimal(4,0), TOTALMALES: int, TOTALFEMALES: int]

  joinedRDD.explain

  //Result
  //== Physical Plan ==
  //SortMergeJoin [cast(ZIPCODE#3 as double)], [cast(zip_code#6 as double)]
  //:- Sort [cast(ZIPCODE#3 as double) ASC], false, 0
  //:  +- TungstenExchange hashpartitioning(cast(ZIPCODE#3 as double),200), None
  //:     +- ConvertToUnsafe
  //:        +- Filter (COUNTY#4 = Los Angeles)
  //:           +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,US_GEOGRAPHY,[Lorg.apache.spark.Partition;@14a75d82,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=US_GEOGRAPHY, driver=oracle.jdbc.driver.OracleDriver})[STATE_FIPS#0,STATE#1,STATE_ABBR#2,ZIPCODE#3,COUNTY#4,CITY#5] PushedFilters: [EqualTo(COUNTY,Los Angeles)]
  //+- Sort [cast(zip_code#6 as double) ASC], false, 0
  //   +- TungstenExchange hashpartitioning(cast(zip_code#6 as double),200), None
  //      +- ConvertToUnsafe
  //         +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,ZIPCODE_POPULATION,[Lorg.apache.spark.Partition;@47e4dd92,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=ZIPCODE_POPULATION})[ZIP_CODE#6,TOTALPOPULATION#7,MEDIANAGE#8,TOTALMALES#9,TOTALFEMALES#10]


  //--Broadcast--

  val joinedRDD = geoDF.filter("county = 'Los Angeles'").join(broadcast(popDF), geoDF("ZIPCODE") === popDF("zip_code"))
  joinedRDD.explain

  //Result
  //== Physical Plan ==
  //BroadcastHashJoin [cast(ZIPCODE#14 as double)], [cast(zip_code#17 as double)], BuildRight
  //:- ConvertToUnsafe
  //:  +- Filter (COUNTY#15 = Los Angeles)
  //:     +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,US_GEOGRAPHY,[Lorg.apache.spark.Partition;@3c0e3dad,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=US_GEOGRAPHY, driver=oracle.jdbc.driver.OracleDriver})[STATE_FIPS#11,STATE#12,STATE_ABBR#13,ZIPCODE#14,COUNTY#15,CITY#16] PushedFilters: [EqualTo(COUNTY,Los Angeles)]
  //+- ConvertToUnsafe
  //   +- Scan JDBCRelation(jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF,ZIPCODE_POPULATION,[Lorg.apache.spark.Partition;@7ad8178,{url=jdbc:oracle:thin:amordenti/amordenti@137.204.78.85:1521/SISINF, dbtable=ZIPCODE_POPULATION})[ZIP_CODE#17,TOTALPOPULATION#18,MEDIANAGE#19,TOTALMALES#20,TOTALFEMALES#21]


  //============================================
  //Exercise 8

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType, StructField, StringType}

  val postcodes = sc.textFile("postcodes/postcodes_uk.csv")
  val schemaString = "postcode county_name ward country_name"

  val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
  val rowRDD = postcodes.map(_.split(",")).map(e ⇒ Row(e(0), e(1), e(2), e(3)))
  val postCodeDF = sqlContext.createDataFrame(rowRDD, schema)

  // Cached computation
  postCodeDF.registerTempTable("postCode_cached")
  sqlContext.cacheTable("postCode_cached")

  val cachedResult = sqlContext.sql("select ward,count(postcode) n_postcodes from postCode_cached group by ward")

  cachedResult.collect()

  // Not cached computation
  postCodeDF.registerTempTable("postCode_not_cached")
}