import org.apache.spark.sql.SparkSession

object ExecPlan1 extends App {

  val spark = SparkSession
    .builder()
    .appName("DateDiff")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  val data1 = Seq(("1", "Java", "20000"),
    ("2", "Python", "100000"),
    ("3", "Scala", "3000"))
  // Create languages DF
  val languages = spark.createDataFrame(data1)
    .toDF("id","language","tution_fees")
  // Create temporary view
  languages.createOrReplaceTempView("languages")


  val data2 = Seq(("1", "studentA"), ("1", "studentB"),
    ("2", "studentA"), ("3", "studentC"))

  // Create students DF
  val students = spark.createDataFrame(data2).toDF("language_id","studentName")

  // Create temporary view
  students.createOrReplaceTempView("students")

  // Join tables
  val df =spark.sql("""SELECT students.studentName, SUM(students.language_id) as c
         FROM students
         INNER JOIN languages
         ON students.language_id= languages.id
         WHERE students.studentName ='studentA'
         group by students.studentName""")

  df.explain(extended = true)

}
