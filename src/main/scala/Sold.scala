import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{col, lag}
import org.apache.spark.sql.{SparkSession, functions}

object Sold extends App {

  val spark = SparkSession
    .builder()
    .appName("Sold")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val data = spark
    .read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv("data/sold.csv")
    .withColumn(
      "running_total",
      functions.sum("items_sold")
        .over(partitionBy("department")
          .orderBy(col("time"))
          //.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )
    .withColumn("diff",
      col("running_total") -
        lag("running_total", 1, 0)
          .over(partitionBy("department")
            .orderBy(col("time"))))
    .orderBy("time")

  data.printSchema()
  data.show()

}
