import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}

object Exploding extends App {

  // Exploding structs array

  val spark = SparkSession.builder()
    .appName("Exploding")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  case class FromTo(open: String, close: String)
  case class Calendar(business_id: String, full_address: String, hours: Map[String, FromTo])

  import org.apache.spark.sql.catalyst.ScalaReflection

  val schema = ScalaReflection.schemaFor[Calendar].dataType.asInstanceOf[StructType]

  val input = spark.read
    .option("multiLine", value = true)
    .schema(schema)
    .json("data/inputExploding.json")

  input.printSchema()
  input.show(truncate = false)

  val hours = explode(col("hours")).as(Seq("day", "value"))
  val columns = input.columns.map(col) :+ hours

  input
    .select(columns :_*)
    .withColumns(Map("open" -> col("value.open"), "close" -> col("value.close")))
    .drop("hours", "value")
    .show()

  input
    .alias("t")
    .selectExpr("t.*", "explode(t.hours) as (day, time)")
    .selectExpr("t.*", "day", "time.*")
    .drop("hours")
    .show()

  val schema2 = new StructType()
    .add("business_id", StringType)
    .add("full_address", StringType)
    .add("hours", MapType(StringType, new StructType()
      .add("open", StringType)
      .add("close", StringType)
    ))

  spark
    .read
    .option("multiline", value = true)
    .schema(schema2)
    .json("data/inputExploding.json")
    .show(truncate = false)

  println(spark.version)

}
