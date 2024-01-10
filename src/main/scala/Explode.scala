import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object Explode extends App {

  val spark = SparkSession
    .builder()
    .appName("Explode")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  case class Nums(nums: Seq[Int])

  val nums = Seq(Seq(1,2,3)).toDF("nums")
  val ds = nums.as[Nums]

  val flat = ds.flatMap{case Nums(nums) => nums.map((nums, _))}.toDF("nums", "num")

  val exp = nums.withColumn("num", explode(col("nums")))

  flat.show()
  exp.show()

  flat.explain()
  exp.explain()

  val resp = scala.io.StdIn.readLine()

}
