import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AddDays extends App {

  val spark = SparkSession
    .builder()
    .appName("AddDays")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val data = Seq(
    (0, "2016-01-1"),
    (1, "2016-02-2"),
    (2, "2016-03-22"),
    (3, "2016-04-25"),
    (4, "2016-05-21"),
    (5, "2016-06-1"),
    (6, "2016-03-21")
  ).toDF("number_of_days", "date")
    .withColumn("future", date_add(to_date(col("date"), "yyyy-MM-d"), col("number_of_days")))

  data.printSchema()
  data.show()

}
