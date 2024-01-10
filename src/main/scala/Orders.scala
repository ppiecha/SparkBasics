import org.apache.spark.sql.SparkSession

object Orders extends App {
  val spark = SparkSession.builder()
    .appName("FreeCourse")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  case class Item(id: Int, name: String, price: Double)

  case class Order(id: Int, itemId: Int, count: Int)

  val items = spark.createDataFrame(
    Seq(
      Item(0, "Tomato", 2.0),
      Item(1, "Watermelon", 5.5),
      Item(2, "pineapple", 7.0)
    )
  )

  val orders = spark.createDataFrame(
    Seq(
      Order(100, 0, 1),
      Order(100, 1, 1),
      Order(101, 2, 3),
      Order(102, 2, 8)
    )
  )

  items.createOrReplaceTempView("items")
  orders.createOrReplaceTempView("orders")

  val report = spark.sql(
    """           select items.name,
      |                  items.price,
      |                  sum(orders.count) as c
      |             from orders
      |             join items
      |               on items.id = orders.itemId
      |            where items.id=2
      |            group by items.name, items.price"""
      .stripMargin
  )

  report.show()
  report.explain("extended")

}
