import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{col, rank}

object BookRank extends App{

  val spark = SparkSession
    .builder()
    .appName("BookRank")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val data = spark
    .read
    .option("header", value = true)
    .csv("data/books.csv")

  data.show()

  data
    .withColumn("rank", rank().over(partitionBy("genre").orderBy(col("quantity").desc)))
    .where(col("rank").isin(1, 2))
    .show()


}
