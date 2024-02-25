import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NonNullValue extends App {

  val spark = SparkSession
    .builder()
    .appName("NonNullValue")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val data = Seq(
    (None, 0),
    (None, 1),
    (Some(2), 0),
    (None, 1),
    (Some(4), 1)).toDF("id", "group")

  data.printSchema()
  data.show()

  data
    .groupBy("group")
    .agg(first("id", ignoreNulls = true).as("first_not_null"))
    .show()


}
