import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object CreateDS extends App {

  val spark = SparkSession
    .builder()
    .appName("CreateDS")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val data = Seq(
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3)
  )

  println(data.getClass.getName)

  import spark.implicits._
  val df = data.toDF("a", "b", "c")
  println(df.getClass.getName)
  df.printSchema

  case class Data(c1: Int, c2: Double, c3: Double)

  val ds = data.toDF("c1", "c2", "c3").as[Data]
  ds.printSchema()
  ds.show()

  val schema = ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType]
  val ds2 = data.map{case (a, b, c) => Data(a, b, c)}
  val ds3 = ds2.toDS()
  ds3.show()
  ds3.printSchema()

}
