import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

object BroadcastVarToDisplayMovieNames {
  case class Movies(userId: Int, movieId: Int, rating:Int, timestamp: Long)
  //Here is just Scala code not Spark code, since our data size is not too big we can use this method
  def loadMovieNames() : Map[Int, String] = {
    //handle character encoding issues - not ut8 just for the data in u.item file
    implicit val codec: Codec = Codec("ISO-8859-1")
    var movieNames: Map[Int, String] = Map()
    var lines = Source.fromFile("data/ml-100k/u.item") //read data from local disc

    for(line <- lines.getLines()){
      val fields = line.split('|')
      if(fields.length >1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("BroadcastVarToDisplayMovieNames").master("local[*]").getOrCreate()
    //create schema
    val moviesSchema = new StructType()
      .add("userId", IntegerType , nullable=true)
      .add("movieId",IntegerType , nullable=true)
      .add("rating", IntegerType,nullable=true)
      .add("timeStamp",LongType , nullable=true)

    import spark.implicits._
    val movieDS = spark.read
      .option("sep","\t")
      .schema(moviesSchema)  //read using this schema
      .csv("data/ml-100k/u.data")
      .as[Movies]

    val popularity = movieDS.groupBy("movieId")
      .count() //create count column
      .orderBy(desc("count"))

    val nameDict = spark.sparkContext.broadcast(loadMovieNames())
    val lookUpName: Int => String = (movieID: Int) => {
      nameDict.value(movieID) //getting map from this obj wth .value
    }
    val lookUpNameUdf = udf(lookUpName) //wrap that func with udf o use sql settings
    //add a column for titles using udf
    val moviesWithNames = popularity.withColumn("movieTitle", lookUpNameUdf(col("movieID")))

    moviesWithNames.show()

  }
}
