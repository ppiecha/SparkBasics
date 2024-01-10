import org.apache.spark.sql.SparkSession

object ReverseDataSetOutput extends App {

  val spark = SparkSession
    .builder()
    .appName("ReverseDataSetOutput")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  spark
    .read
    .option("comment", "+")
    .option("delimiter", "|")
    .option("header", value = true)
    .csv("data/DataSetOutput.csv")
    .drop("_c0", "_c4")
    .show()
}
