import FirendsByAgeWithDataset.Friends
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object WordCountWithDataset {
  case class Word(value: String)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession.builder.appName("WordCountWithDataset").master("local[*]").getOrCreate()
    //data load
    import ss.implicits._
    val dataset = ss.read.text("data/book.txt").as[Word]
    //using regular expression in explode. Explode is similar to flatmap
    val words = dataset.select(explode(split($"value","\\W+")).alias("words")).filter($"words" =!= "")

    val lowerCaseWords=words.select(lower($"words").alias("words"))
    //count age amounts from selected data
    val counts = lowerCaseWords.groupBy("words").count()
    val countSorted = counts.sort("count")
    countSorted.show(counts.count.toInt)
  }
}
