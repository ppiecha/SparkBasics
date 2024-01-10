import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateTable1 extends App {

  //enableHiveSupport() -> enables sparkSession to connect with Hive
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkCreateTableExample")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val data = Seq(
    (1, "one"),
    (2, "two"),
    (3, "three")
  )

  val df = data.toDF("id", "name")

  df.createTempView("sampleView")

  spark.sql("create database if not exists db")

  spark.sql("create table if not exists db.sample(id Int, name String)")

  spark.sql("truncate table db.sample")

  spark.sql("insert into table db.sample select * from sampleView")

  spark.sql("select * from db.sample").show()

  df
    .write
    .mode(SaveMode.Overwrite)
    .saveAsTable("db.sample2")

  val df2 = spark.read.table("db.sample2")

  df2.printSchema()
  df2.show()

  df2.write
    .mode(SaveMode.Overwrite)
    .partitionBy("id")
    .option("path", "C:\\Users\\e-prph\\My Private Documents\\git\\scala\\spark\\FreeCourse\\data\\sample")
    .saveAsTable("db.sample3")

  spark.catalog.listDatabases().show(false)
  spark.catalog.listTables("db").show(false)
  val cols = spark.catalog.listColumns("db.sample2")
  cols.printSchema()
  cols.show(false)

  spark.sql("show partitions db.sample3").show()

  spark.sql("describe db.sample3").show()

  spark.sql("select version()").show(false)

}
