import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{col, first}

object Salaries extends App {

  val spark = SparkSession
    .builder()
    .appName("Salaries")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val data = spark
    .read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv("data/salaries.csv")
    .withColumn(
      "max",
      first("salary").over(partitionBy("department").orderBy(col("salary").desc))
    )
    .withColumn("diff", col("max") - col("salary"))
    .drop("max")

  data.printSchema()
  data.show()




}
