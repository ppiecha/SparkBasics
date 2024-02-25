import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{col, expr, rank}

object LongestSequence extends App {

  val spark = SparkSession
    .builder()
    .appName("LongestSequence")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val data = spark
    .read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv("data/sequence.csv")

  data.printSchema()
  data.show()

  data
    .withColumn("rank", rank().over(partitionBy(col("id")).orderBy("time")))
    .withColumn("diff", col("time") - col("rank"))
    .groupBy("id", "diff")
    .count()
    .groupBy("id")
    .agg(functions.max("count").as("time"))
    .show()

}
