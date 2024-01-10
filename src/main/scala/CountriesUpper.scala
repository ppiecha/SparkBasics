import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}

object CountriesUpper extends App {

  val spark = SparkSession
    .builder()
    .appName("CountriesUpper")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val path = args.head
  val cols = args.tail

  spark
    .read
    .option("header", value = true)
    .csv(path)
    .withColumns(cols.map(c => (s"upper_$c", functions.upper(col(c)))).toMap)
    .show()

}
