import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object MostPopularMovie {
  final case class Movie (movieId: Int)

  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("MostPopularMovie").master("local[*]").getOrCreate()

    //create schema
    val moviesSchema = new StructType()
      .add("userId", IntegerType , nullable=true)
      .add("movieId",IntegerType , nullable=true)
      .add("rating", IntegerType,nullable=true)
      .add("timeStamp",LongType , nullable=true)

    //load the data
    import spark.implicits._
    val movieDS = spark.read
      .option("sep","\t")
      .schema(moviesSchema)  //read using this schema
      .csv("data/ml-100k/u.data")
      .as[Movie] //dataset doesn't have to include all the columns, we ask for only movieId column

    val popularity = movieDS.groupBy("movieId")
      .count() //create count column
      .orderBy(desc("count"))

    popularity.show(20)

    spark.stop()
  }
}
