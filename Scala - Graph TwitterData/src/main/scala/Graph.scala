import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD 

object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5
 
  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    val graph: RDD[(Int,Int)]
         = sc.textFile(args(0))
             .map( line => {val id = line.split(",")
                            (id(1).toInt,id(0).toInt)} )           // create a graph edge (i,j), where i follows j

    var R: RDD[(Int,Int)]             // initial shortest distances
         = graph.groupByKey()
                .map{
                  case(follower,users) =>
                  if (follower == start_id || follower == 1){
                    (follower,0)
                  }else
                  {
                    (follower,max_int)
                  }

                 }          // starting point has distance 0, while the others max_int



    for (i <- 0 until iterations) {
        R = R.join(graph)
       .flatMap {
         case (follower, (distance, id)) =>
           if (distance < max_int) {
             // Two alternatives: (1) Current distance or (2) Current distance + 1
             List((follower, distance), (id, distance + 1))
           } else {
             // If the current distance is already maximum, keep it the same
             List((follower, distance))
           }
       }
       .reduceByKey((currentDistance, newDistance) => Math.min(currentDistance, newDistance))
}


    R.filter
      {
         case (follower, distance) => distance < max_int

      }                     // keep only the vertices that can be reached
     .map{
      case (follower, distance) => (distance, 1)
        }                     // prepare for the reduceByKey
     .reduceByKey(_+_)               // for each different distance, count the number of nodes that have this distance
     .sortByKey()
     .collect()
     .foreach(println)
    
  }
}
