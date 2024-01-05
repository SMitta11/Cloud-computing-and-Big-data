import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._

object Netflix {
  case class Rating ( movieID: Int, userID: Int, rating: Int, date: String  )

  case class Movie ( movieID: Int, year: Int, title: String )

  def main ( args: Array[ String ] ): Unit = {
    val conf = new SparkConf().setAppName("Netflix")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

  val ratings: RDD[Rating] = sc.textFile(args(0))
                               .map(_.split(",")).map( n => Rating(n(0).toInt,n(1).toInt,n(2).toInt,
                               n(3).toString))
    val movies: RDD[Movie] = sc.textFile(args(1))
                             .map(_.split(",")).map(n => Movie(n(0).toInt,try {n(1).toInt} catch {
                              case _: NumberFormatException => 0 // Handle null or non-integer values as 0
                              }
                              ,n.drop(2).mkString(",")))
  

    ratings.toDF.createOrReplaceTempView("R")
    movies.toDF.createOrReplaceTempView("M")

    val result = spark.sql("""
                           SELECT CAST(M.year AS STRING)  AS year ,
                                  REPLACE(M.title, ',', '') AS title,
                                  CAST(AVG(R.rating) AS DOUBLE) AS avg_rating
                           FROM M JOIN R
                           ON M.movieID = R.movieID
                           GROUP BY M.year,title
                           ORDER BY  avg_rating DESC
                           LIMIT 100
                           /* ... */
                           """)



    result.collect.foreach { case Row( year: String, title: String, avg: Double )
                               => println("%1.3f\t%s   %s".format(avg,year,title)) }

}
}
