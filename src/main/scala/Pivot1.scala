import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SparkSession, functions}

object Pivot1 extends App {

  val spark = SparkSession
    .builder()
    .appName("Pivot1")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val data = spark.read
    .option("header", value = true)
    .csv("data/pivot1.csv")

  data.show()

  data.groupBy(lit("cc").as("udate"))
    .pivot("udate")
    .agg(functions.max("cc"))
    .show()

}
