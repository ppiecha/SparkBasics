import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object IdsWithWord extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("IdsWithWord")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val input = spark
    .read
    .option("header", value = true)
    .csv("data/IdsWithWords.csv")

  input.printSchema()
  input.show()

  val splitted = input
    .withColumn("arr", functions.split(col("words"), ","))
  val exploded = splitted.select(col("id"), explode(col("arr")).as("word"))

  exploded
    .show()

  input
    .select(col("word"))
    .join(exploded, input("word") === exploded("word"), "inner")
    .groupBy(exploded("word"))
    .agg(collect_list(col("id")).as("ids"))
    .show()

}
