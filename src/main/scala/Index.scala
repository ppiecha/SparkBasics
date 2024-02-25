import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{ArrayType, StructType}

object Index extends App {

  val spark = SparkSession
    .builder()
    .appName("Index")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val data = Seq("one", "two", "three").toDF("name")

  println(spark.version)

  val df = data
    .withColumn("id", row_number().over(partitionBy().orderBy("name")))

  df.printSchema()
  df.show()

  val seq1 = Seq("a", "b", "c", "d")
  val seq2 = Seq(1, 2, 3, 4)

  val df1 = seq1.zip(seq2).toDF("col1", "col2")
  val df2 = seq1.zip(seq2).zipWithIndex.toDF("col1", "col2")

  df1.printSchema()
  df1.show()

  df2.printSchema()
  df2.show()

  val cols = Seq(("col1", "c1"), ("col2", "c2")).toMap

  df2.withColumnsRenamed(cols).show()

}
