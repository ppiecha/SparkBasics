import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FreeCourse")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")


    //df.show()
    //df.printSchema()
    //df.explain("formatted")

    df.select("Date", "Open", "Close")
    val column1 = df("Date")
    val column2 = col("Date")
    import spark.implicits._
    val column3 = $"Date"
    df.select(df("Date"), col("Open"), $"Close")
    df.select($"Open" + 2, $"Date".cast(StringType).as("asString"), $"Open" === $"Close")
      .where($"Open" === $"Close")
      //.show()

    df.select(concat(col("Date"), lit(" a")))
      //.show()

    df.select(date_trunc("second", current_timestamp()))
      //.show(truncate = false)

    df
      .withColumn("diff", col("Open") - col("Close"))
      .filter(col("Close") > col("Open") * 1.1)
      //.show()

    df
      .groupBy(year($"Date").as("year"))
      .agg(max($"Close"), avg($"Close"))
      .sort($"year".desc)
      .show()

    val window = Window.partitionBy(year($"Date").as("year")).orderBy($"Close".desc)
    df
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .sort($"Close".desc)
      .show()

  }
}