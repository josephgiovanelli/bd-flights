class Esercizi {

  // Local init
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
  val customerDF = hiveContext.read.table("customers")
  // Or
  val rows = hiveContext.sql("SELECT key, value from mytable")
  val keys = rows.map(row => row.getInt(0))

  // ------- PARQUET -------
  val usersDF = sqlCtx.read.load("examples/src/main/resources/users.parquet")
  //or
  val sqlDF = sqlCtx.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")

  // ------- JSON -------
  val dfJSON = sqlCtx.jsonFile("example/path/test.json")
  //or
  val peopleDF = sqlCtx.read.format("json").load("examples/path/test.json")

  // ------- From RDD -------
  import org.apache.spark.sql.Row

  val dummyRDD = sc.parallelize([Row(name="George", surname="Washington")])
  val dummyDataFrame = sqlCtx.createDataFrame(dummyRDD)

  // ------- Manually from a DataSource -------
  sqlCtx.read.format("jdbc")
    .option("url","jdbc:{postgre,oracle,mysql}://hostAddress")
    .option("dbtable","accounts")
    .option("user", "username")
    .option("password","password")
    .load()

  // select
  sqlCtx.sql("select * from customers where name='Steve' ")
  
  // Schema
  val people = sc.textFile("people.txt")
  val schemaString = "name surname age"
  // Import respective APIs
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType,StructField,StringType}
  //Generate schema
  val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
  // Apply transformation for reading data
  val rowRDD = population.map(_.split(",")).map(e => Row(e(0), e(1), e(2)))
  // Create DataFrame and apply the schema to it
  val peopleDF = sqlCtx.createDataFrame(rowRDD, schema)
  dfPeople.registerTempTable("people")
  val output = sqlCtx.sql("select * from people")
  output.show()

  /*
   * Exercise 1
   */
  // DataFrame movies
  val dfMovies = sqlCtx.jsonFile("/bigdata/dataset/movies")

  // DataFrame population
  val population = sc.textFile("/bigdata/dataset/population/zipcode_population_no_header.csv")
  // zipcode, average age, total population and both male and female figures
  val schemaString = "zipcode averageAge totalPopulation maleFigure femaleFigure"
  // Import respective APIs
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StructType,StructField,StringType}
  //Generate schema
  val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
  // Apply transformation for reading data
  val rowRDD = population.map(_.split(";")).map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4)))
  // Create DataFrame and apply the schema to it
  val dfPopulation = sqlCtx.createDataFrame(rowRDD, schema)
  dfPopulation.registerTempTable("people")

//  // ------ DataFrame population
//  // Create custom schema
//  val schemaString = "zipcode averageAge totalPopulation maleFigure femaleFigure"
//  // Import respective APIs
//  import org.apache.spark.sql.Row
//  import org.apache.spark.sql.types.{StructType,StructField,StringType}
//  //Generate schema
//  val schemaPopulation = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
//  // Create df with custom schema from csv without header
//  val dfPopulation = sqlCtx.read
//    .format("csv")
//    .option("header", "false") //first line in file has not headers
//    .option("mode", "DROPMALFORMED")
//    .schema(schemaPopulation)
//    .load("hdfs:///bigdata/dataset/population")

  // DataFrame real_estate
  val dfRealEstate = sqlCtx.read
    .format("csv")
    .option("delimiter", ";")
    .option("header", "true") //first line in file has headers
    .option("mode", "DROPMALFORMED")
    .load("hdfs:///bigdata/dataset/real_estate/real_estate_transactions.txt")

  // DataFrame userdata
  val dfUsersdata = sqlCtx.read.load("/bigdata/dataset/userdata")

  /*
   * Exercise 2
   */
  // first
  dfPopulation.write.format("json").save("i5Population")

  // second
  dfUsersdata.createOrReplaceTempView("i5UserdataTemp")
  sqlCtx.sql("create table i5Userdata as select * from i5UserdataTemp")
  // or
  dfUsersdata.write.saveAsTable("i5Userdata")

  // third
  dfMovies.write.format("parquet").save("i5Movies")

  /*
   * Exercise 3
   */

  /*
   * Exercise 4
   */

  /*
   * Exercise 5
   */

  /*
   * Exercise 6
   */

  /*
   * Exercise 7
   */

  /*
   * Exercise 8
   */

}
