import org.apache.spark.sql._

object Merge  extends App {

  val spark = SparkSession.builder()
    .appName("CollectSet")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val input = Seq(
    ("100", "John", Some(35), None),
    ("100", "John", None, Some("Georgia")),
    ("101", "Mike", Some(25), None),
    ("101", "Mike", None, Some("New York")),
    ("103", "Mary", Some(22), None),
    ("103", "Mary", None, Some("Texas")),
    ("104", "Smith", Some(25), None),
    ("105", "Jake", None, Some("Florida"))).toDF("id", "name", "age", "city")

  input.show()

  input.createTempView("input")
  spark.sql(
      """
        |select id,
        |       max(name) as name,
        |       max(age) as age,
        |       max(city) as city
        |  from input in1
        | group by id
        |""".stripMargin)
    .show(truncate = false)


}
