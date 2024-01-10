import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object DateDiff extends App {

  val spark = SparkSession
    .builder()
    .appName("DateDiff")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val dates = Seq(
    "08/12/2023",
    "09/12/2023",
    "08/01/2024"
  )
    .toDF("date_string")
    .withColumn("to_date", to_date(col("date_string"), "dd/MM/yyyy"))
    .withColumn("diff", date_diff(current_date(), col("to_date")))

  dates.show()

}
