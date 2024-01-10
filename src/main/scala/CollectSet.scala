import org.apache.spark.sql._

object CollectSet extends App {

  val spark = SparkSession.builder()
    .appName("CollectSet")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._
  val input = spark.range(50).withColumn("key", $"id" % 5)
  input.show()

  input.createTempView("input")
  spark.sql(
    """
      |select key,
      |       collect_set(id) as all,
      |       filter(collect_set(id), (x, i) -> i <= 2) as only_first_three
      |  from input
      | group by key
      |""".stripMargin)
    .show(truncate = false)

}
