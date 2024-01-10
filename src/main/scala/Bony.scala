import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Bony {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Bony")
      .master("local[*]")
      .config("spark.driver.bindAddress", "localhost")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val path = args.headOption match {
      case Some(value) => value
      case None => throw new Error("No args provided")
    }

    val schema = new StructType()
      .add("first", StringType)
      .add("last", StringType)
      .add("year", IntegerType)

    println(spark.version)

    val df = spark
      .read
      .schema(schema)
      .csv(path)

    df.printSchema()
    df.show()

  }
}
//set SPARK_LOCAL_IP="127.0.0.1"
//spark-submit --deploy-mode client --master local[*] --class "Bony" --conf "spark.driver.bindAddress=localhost" target/scala-2.13/freecourse_2.13-0.1.0-SNAPSHOT.jar data/bony.csv
//spark-submit --master local[*] --class "Bony" target/scala-2.13/freecourse_2.13-0.1.0-SNAPSHOT.jar
//final set sbt scala version to the same as spark scala varsion 2.12.18
//spark-submit --master local[*] --class "Bony" target/scala-2.12/freecourse_2.12-0.1.0-SNAPSHOT.jar data/bony.csv
