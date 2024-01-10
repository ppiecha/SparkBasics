import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}

object Partitions1 extends App {

  val spark = SparkSession
    .builder()
    .appName("Partitions1")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val schema = new StructType()
    .add("date", DateType)
    .add("open", DoubleType)
    .add("high", DoubleType)
    .add("low", DoubleType)
    .add("close", DoubleType)
    .add("adj_close", DoubleType)
    .add("volume", DoubleType)

  val df = spark.read
    .option("header", value = true)
    .schema(schema)
    .csv("data/AAPL.csv")
    .withColumn("dateYear", functions.year(functions.to_date(col("date"), "yyyy-MM-dd")))

  df.printSchema()
  df.describe( "dateYear").show(false)
  df.show()

  df
    .groupBy("dateYear")
    .agg(count(col("*")))
    .orderBy("dateYear")
    .show(50)

  //val df2 = df.repartitionByRange($"date")
  //println(df2.rdd.getNumPartitions)

  df
    .write
    .mode(SaveMode.Overwrite)
    .partitionBy("dateYear")
    .parquet("data/parquet/aapl")

  spark.sql("show tables").show()

}
