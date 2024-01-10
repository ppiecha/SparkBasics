import org.apache.spark.sql._

object SplitFunction extends App {

  val spark = SparkSession.builder()
    .appName("SplitFunction")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")
  )
    .toDF("values", "delimiter")

  val res = dept
    .withColumn("split_values", functions.expr("split(values, concat('\\\\', delimiter))"))
    .withColumn("extra", functions.expr("filter(split_values, x -> x != '' )"))

  res.show(truncate = false)
  res.printSchema()

}
