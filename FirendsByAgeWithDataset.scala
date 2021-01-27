import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FirendsByAgeWithDataset {
  case class Friends (id: Int, name:String , age:Int,friends : Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession.builder.appName("FirendsByAgeWithDataset").master("local[*]").getOrCreate()
    //data load
    import ss.implicits._
    val dataset = ss.read.
      option("header","true").option("inferSchema","true").csv("data/fakefriends.csv").as[Friends]

    val friendsByAge = dataset.select("age","friends") //excel headers

    //group by selected data
    friendsByAge.groupBy("age").avg("friends") //.show()

    //format
    friendsByAge.groupBy("age").agg(round(avg("friends"),2)).sort("age").show()
  }
}
