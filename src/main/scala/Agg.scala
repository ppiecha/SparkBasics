import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Agg extends App {

  val spark = SparkSession
    .builder()
    .appName("Agg")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  if (args.length < 2) throw new RuntimeException("Not enough params")

  val (path, func) = (args.head, args.tail)

  val data = spark
    .read
    .option("header", value = true)
    .csv(path)
    .withColumn("pop", array_join(split(col("population"), " "), "").cast(IntegerType))

  data.printSchema()
  data.show()

  val aggCols = func.zipWithIndex.map{case (name, id) => expr(s"""$name(pop)""").as(s"a$id")}.toSeq

  val df = data.agg(aggCols.head, aggCols.tail: _*)

  val value = df.collect().head.getInt(0)

  val df2 =
    if (aggCols.length == 1)
      //df.withColumn("city", lit(data.select("name").where(col("pop") === value).collect().head.getString(0)))
      df.join(data, data("pop") === df("a0")).select(col("a0"), col("name").as("city"))
    else
      df

  df2.show()

}
