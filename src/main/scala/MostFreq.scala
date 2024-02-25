import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowSpec
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, last_value, lit}

object MostFreq extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MostFreq")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val input = Seq(
    (1, "Mr"),
    (1, "Mme"),
    (1, "Mr"),
    (1, null),
    (1, null),
    (1, null),
    (2, "Mr"),
    (3, null)).toDF("UNIQUE_GUEST_ID", "PREFIX")

  input
    .groupBy("UNIQUE_GUEST_ID", "PREFIX")
    .count()
    .withColumn("rank", last_value(col("prefix"), lit(true)).over(Window.partitionBy("UNIQUE_GUEST_ID").orderBy("count")))
    //.groupBy("UNIQUE_GUEST_ID")
    //.agg(first("PREFIX", ignoreNulls = true))
    .show()

}
