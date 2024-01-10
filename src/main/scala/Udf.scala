import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}

object Udf extends App {

  val spark = SparkSession
    .builder()
    .appName("Udf")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val data = Seq(
    (1, "one"),
    (2, "tWo"),
    (3, "threE")
  ).toDF("id", "name")

  val myUpperCase = (s: String) => s.toUpperCase

  val sqlUpperCase = functions.udf(myUpperCase)

  spark.udf.register("sqlUpperCase", myUpperCase)

  data.withColumn("upper", sqlUpperCase(col("name"))).show()

  data.createTempView("data")

  spark.sql("select id, name, sqlUpperCase(name) as upper from data").show()

}
