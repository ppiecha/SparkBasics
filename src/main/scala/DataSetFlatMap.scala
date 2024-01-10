import org.apache.spark.sql._

object DataSetFlatMap extends App {

  // Using Dataset.flatMap Operator

  val spark = SparkSession
    .builder()
    .appName("DataSetFlatMap")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val nums = Seq(Seq(1,2,3)).toDF("nums")

  nums.show()

  case class Nums(nums: Seq[Int])

  val ds = nums.as[Nums]

  ds.flatMap{case Nums(nums) => nums.map((nums, _))}.toDF("nums", "num").show()


}
