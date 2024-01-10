import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, expr, replace}
import org.apache.spark.sql.types.IntegerType

object Countries extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Countries")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val df = spark
    .read
    .option("header", value = true)
    .csv("data/countries.csv")

  val df2 = df
    .withColumn("pop", expr("replace(population, ' ', '')").cast(IntegerType))
    .drop("population")
  df2.printSchema()
  df2.show()

  df2
    .groupBy("country")
    .agg(functions.max("pop").as("max_pop").as("max_pop"))
    .alias("grouped")
    .join(
      df2.alias("source"),
      col("source.pop") === col("grouped.max_pop") &&
      col("source.country") === col("grouped.country"),
      "inner"
    )
    .select("source.*")
    .show()

}
