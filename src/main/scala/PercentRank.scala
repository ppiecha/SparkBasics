import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{col, percent_rank, when}

object PercentRank extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("PercentRank")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val data = spark
    .read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv("data/PercentRank.csv")
    .withColumn("rank", percent_rank().over(partitionBy().orderBy(col("salary").desc)))
    .withColumn("Percentage",
      when(col("rank") <= 0.3, "High")
        .when(col("rank") <= 0.4 && col("rank") > 0.3, "Average")
        .otherwise("Low")
    )
    //.drop("rank")

  data.printSchema()
  data.show()

}
