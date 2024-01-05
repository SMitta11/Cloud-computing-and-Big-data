
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Netflix {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Netflix")
    val sc = new SparkContext(conf)

    val ratings: RDD[(Int, Int, Int)] = sc.textFile(args(0))
      .map(line => {
        val a = line.split(",")
        (a(0).toInt, a(1).toInt, a(2).toInt)
      })

    // Get the movies data from file and handle null in the year field
    val movies: RDD[(Int, Int, String)] = sc.textFile(args(1))
      .map(line => {
        val parts = line.split(",")
        val id = parts(0).toInt
        val year = try {
          parts(1).toInt
        } catch {
          case _: NumberFormatException => 0 // Handle null or non-integer values as 0
        }

        // Combine all parts of the title starting from the 3rd part onward
        val title = parts.drop(2).mkString(",")

        (id, year, title)
      })

    // Need data of ratings only for movieid and ratings
    val ratings_data = ratings.map {
      case (movieid, _, rating) => (movieid, rating)
    }

    // Calculate the sum of ratings for each movie
    val summedRatings = ratings_data
      .reduceByKey {
        case (sum1, sum2) => sum1 + sum2
      }

    // Calculate the count of ratings for each movie
    val countRatings = ratings_data
      .map {
        case (movieID, rating) => (movieID, 1)
      }
      .reduceByKey {
        case (count1, count2) => count1 + count2
      }

    // Get the average rating
    val avgRatings = summedRatings.join(countRatings)
      .map {
        case (movieID, (totalRating, count)) =>
          val avgRating = totalRating.toDouble / count
          (movieID, avgRating)
      }

    // Join avgRatings with movies
    val joined_rating_title = avgRatings.map { case (movieID, avgRating) => (movieID, avgRating) }
      .join(movies.map { case (movieID, year, title) => (movieID, (year, title)) })
      .map { case (movieID, (avgRating, (year, title))) => (avgRating, (year, title)) }

    // Format data and handle null in year and concatenated year and title
    /*
    val formattedData: RDD[(Double, String)] = joined_rating_title.map {
      case (avgRating, (year, title)) =>
        // Handle null in year
        val formattedYear = if (year == 0) "0" else year.toString

        val split_title = title.split(",") // Split the text by commas
        val concatenate_title = split_title.map(_.trim).mkString(" ") // Join and trim the parts with spaces
        val formattedTitle = s"$formattedYear: $concatenate_title"
        (avgRating, formattedTitle)
    }
*/
    
    val formattedData: RDD[(Double, String)] = joined_rating_title.map {
      case (avgRating, (year, title)) =>
        // Handle null in year
        val formattedYear = if (year == 0) "0" else year.toString

        val formattedTitle = s"$formattedYear: $title"
        (avgRating, formattedTitle)
    }

    val avg_titles: RDD[(Double, String)] = formattedData.sortByKey(ascending = false)


    avg_titles.coalesce(1).map { case (avg,title) => "%.2f\t%s".format(avg,title) }
              .saveAsTextFile(args(2))

    sc.stop()
  }
}

