import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object CustomerOrdersWithDataset {
  case class custOrder (cust_id: Int, item_id:Int, amount_spent:Double)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //create schema when reading customer orders
    val custOrderSchema = new StructType()
      .add("cust_id",IntegerType, nullable=true)
      .add("item_id",IntegerType, nullable=true)
      .add("amount_spent",DoubleType, nullable=true)

    val ss = SparkSession.builder.appName("FirendsByAgeWithDataset").master("local[*]").getOrCreate()
    //data load
    import ss.implicits._
    val dataset = ss.read.
      schema(custOrderSchema).csv("data/customer-orders.csv").as[custOrder]
    val cust_spent = dataset.select("cust_id","amount_spent")
    val totalByCustomer = cust_spent.groupBy("cust_id").agg(round(avg("amount_spent"),2)
      .alias("total_spent"))
    totalByCustomer.sort("total_spent").show()

  }
}
