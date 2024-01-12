import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first

object Pivot4 extends App {

  val spark = SparkSession
    .builder()
    .appName("Pivot4")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val data = spark
    .read
    .option("header", value = true)
    .csv("data/pivot4.csv")

  data.show()

  data
    .groupBy("key")
    .pivot("date")
    .agg(first("val1").as("v1"), first("val2").as("v2"))
    .show()

}
