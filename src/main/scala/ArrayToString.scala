import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_join, col}

object ArrayToString extends App {

  val spark = SparkSession
    .builder()
    .appName("ArrayToString")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val words = Seq(Array("hello", "world")).toDF("words")
    .withColumn("solution", array_join(col("words"), " "))

  words.show()

}
