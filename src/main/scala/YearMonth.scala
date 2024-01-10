import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object YearMonth extends App {

  val spark = SparkSession
    .builder()
    .appName("YearMonth")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val df = spark
    .read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv("data/dates.csv")
    .toDF("year_month", "amount")
    .withColumn("ym", to_date(col("year_month"), "yyyyMM"))
    .withColumn("nvl", nvl(col("year_month"), lit(0)))

  val months = spark
    .range(-22, 1)
    .withColumn("month0", to_date(lit("202001"), "yyyyMM"))
    .withColumn("month", add_months(col("month0"), col("id")))

  df.printSchema()
  df.show()

  months.printSchema()
  months.show(30)

  val month = date_format(months("month"), "yyyyMM").as("year_month")

  months
    .join(df, df("ym") === months("month"), "left")
    .groupBy(month)
    .agg(sum(expr("nvl(amount, 0)")).as("amount"))
    .orderBy(month.desc)
    .show(30)

}
