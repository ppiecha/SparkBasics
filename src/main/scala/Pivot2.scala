import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{array, avg, col, collect_list}

object Pivot2 extends App {

  val spark = SparkSession
    .builder()
    .appName("Pivot2")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val data = Seq(
    (0, "A", 223, "201603", "PORT"),
    (0, "A", 22, "201602", "PORT"),
    (0, "A", 422, "201601", "DOCK"),
    (1, "B", 3213, "201602", "DOCK"),
    (1, "B", 3213, "201601", "PORT"),
    (2, "C", 2321, "201601", "DOCK")
  ).toDF("id", "type", "cost", "date", "ship")

  data.groupBy("id", "type")
    .pivot("date")
    .agg(avg("cost"))
    .show()

  data.groupBy("id", "type")
    .pivot("date")
    .agg(collect_list("ship"))
    .show()

  val data2 = Seq(
    (100, 1, 23, 10),
    (100, 2, 45, 11),
    (100, 3, 67, 12),
    (100, 4, 78, 13),
    (101, 1, 23, 10),
    (101, 2, 45, 13),
    (101, 3, 67, 14),
    (101, 4, 78, 15),
    (102, 1, 23, 10),
    (102, 2, 45, 11),
    (102, 3, 67, 16),
    (102, 4, 78, 18)).toDF("id", "day", "price", "units")

  val piv1 = data2.groupBy("id")
    .pivot("day")
    .agg(functions.first("price").as("price"), functions.first("units").as("unit"))

  piv1.show()

}
