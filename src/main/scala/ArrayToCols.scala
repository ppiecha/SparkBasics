import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, element_at, explode, posexplode}

object ArrayToCols extends App {

  val spark = SparkSession
    .builder()
    .appName("ArrayToCols")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val input = Seq(
    Seq("a", "b", "c"),
    Seq("X", "Y", "Z"),
    Seq("1", "2", "3", "4"))
    .toDF

  case class Values(value: Seq[String])

  val ds = input.as[Values]
  var df = ds.toDF()

  val first_row_length = ds.collect().head.value.size
  val seq = ds("value")
  val max_size = ds.select(functions.max(functions.size(seq))).collect().head.getInt(0)
  println(max_size)

//  for (i <- 1 to max_size)
//    df = df.withColumn((i - 1).toString, element_at(df("value"), i))
//
//
//  df.drop("value").show()

  val cols = (1 to max_size)
    .map(i => element_at(col("value"), i).as((i - 1).toString))
  ds.select(cols :_*).show()

//  val seq = ds("value")
//  val max_size = ds.select(functions.max(functions.size(seq))).collect().head.getInt(0)
//  val columns = (1 to max_size).map(element_at(seq, _))
//  ds
//    .select(posexplode(seq))
//    .groupBy(seq)
//    .pivot("pos")
//    .agg(functions.max("pos"))
//    .show()

//  ds.
//    select(seq, posexplode(seq))
//    .show()
//
//  ds.
//    select(seq, posexplode(seq))
//    .groupBy("pos")
//    .pivot("pos")
//    .agg(functions.first("col"))
//    .show()

//  ds.select(
//    (0 until 3).map(i => col("value")(i).alias(s"$i")): _*
//  ).show()

}
