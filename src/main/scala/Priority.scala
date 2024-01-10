import org.apache.spark.sql._

object Priority extends App {

  val spark = SparkSession.builder()
    .appName("Priority")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val input = Seq(
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others")).toDF("id", "value")

  input.show()

  input.createTempView("input")
  val sql =
    """
      |select id,
      |       first_value(value) as name
      |  from input
      | group by id
      | order by id
      |""".stripMargin

  spark.sql(sql).show()

}
