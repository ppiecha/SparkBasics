import org.apache.spark.sql.functions.{col, sort_array}
import org.apache.spark.sql.{SparkSession, functions}

object GroupBy extends App {

  val spark = SparkSession
    .builder()
    .appName("GroupBy")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val nums = spark.range(5).withColumn("group", 'id % 2)

  nums.show()

  nums.groupBy("group")
    .agg(functions.max("id").as("max_id"))
    .show()

  nums.groupBy("group")
    .agg(sort_array(functions.collect_list(col("id")), asc = false).as("ids"))
    .show()

  nums.groupBy("group")
    .agg(
      functions.max("id"),
      functions.min("id"),
      functions.collect_list(col("id"))
    ).show()

}
