import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, first, lit}

object Pivot3 extends App {

  val spark = SparkSession
    .builder()
    .appName("Pivot3")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val data = spark
    .read
    .option("header", value = true)
    .csv("data/pivot3.csv")
    .withColumn("label", concat(lit("Qid_"), col("Qid")))

  data.show()

  data
    .groupBy("ParticipantID", "Assessment", "GeoTag")
    .pivot("label")
    .agg(first("AnswerText"))
    .show()

}
