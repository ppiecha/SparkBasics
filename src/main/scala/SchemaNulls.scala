import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SchemaNulls extends App {

  val spark = SparkSession
    .builder()
    .appName("SchemaNulls")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val schema = new StructType()
    .add("date", TimestampType)
    .add("ip", StringType)

  val df = spark
    .read
    .schema(schema)
    .option("sep", "|")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss,SSS")
    .csv("data/timestamp.csv")


  df.printSchema()
  df.show(false)


}
