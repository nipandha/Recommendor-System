sc.setLogLevel("ERROR")
import org.apache.spark.HashPartitioner
import scala.util.parsing.json.JSON
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext


val b_unparsed = sc.textFile("hdfs:///user//nvp263//yelp//business.json")
val b_parsed = b_unparsed.map(line => JSON.parseFull(line).get.asInstanceOf[Map[String,String]])


def show_business_string(x: Option[Any]) = x match {
case None => "?" 
case _ => x.toString.drop(5).dropRight(1)           
}

def show_business_list(x: Option[Any]) = x match {
case None => {List[String]()}
case Some(s) => {s.asInstanceOf[List[String]]}
}

val businesses = b_parsed.map(fields=>(show_business_string(fields.get("business_id")),
show_business_string(fields.get("name")),show_business_string(fields.get("city")),
show_business_string(fields.get("state")), show_business_list(fields.get("categories")),
show_business_string(fields.get("stars")).toDouble))

val businesses_filtered = businesses.filter(_._4=="NY")
val businesses_list = businesses_filtered.map(_._1).take(2000)
def show(x: Option[String]) = x match {
case None => "?" 
case _ => x.toString.drop(5).dropRight(1)                    
}

val users_unparsed = sc.textFile("hdfs:///user//nvp263//yelp//user.json")
val users_parsed = users_unparsed.map(line => JSON.parseFull(line).get.asInstanceOf[Map[String,String]])
val users = users_parsed.map(fields=>(show(fields.get("user_id")),show(fields.get("name")),show(fields.get("review_count")).toDouble.toInt,
 -show(fields.get("yelping_since")).dropRight(6).toDouble.toInt, 
show(fields.get("average_stars")).toDouble))


val reviews_unparsed = sc.textFile("hdfs:///user//nvp263//yelp//review.json")
val reviews_parsed = reviews_unparsed.map(line => JSON.parseFull(line).get.asInstanceOf[Map[String,String]])
val reviews=reviews_parsed.map(fields=>(show(fields.get("review_id")),
show(fields.get("user_id")),show(fields.get("business_id")),
show(fields.get("text")),show(fields.get("stars")).toDouble))
val reviews_to_consider = reviews.filter(fields => businesses_list.contains(fields._3))
val n = businesses_list.size

var user_vectors = collection.mutable.Map[String,Array[Double]]()
def add_vector(uid:String , bid: String , rating:Double ) = {
var vec = Array.fill(n.toInt)(0.0)
val i = businesses_list.indexOf(bid)
if(user_vectors.exists(_._1 == uid)) {
vec = user_vectors(uid)
vec(i) = rating
user_vectors(uid) = vec
}
else {
vec(i) = rating
user_vectors += (uid->vec)
}
}
val v = reviews_to_consider.map(fields => add_vector(fields._2,fields._3,fields._5))
v.count()
val vectors = sc.parallelize(user_vectors)
vectors.map(_._2).saveAsTextFile("hdfs:///user//nvp263//yelp_interim//f1.txt")
vectors.map(_._1).saveAsTextFile("hdfs:///user//nvp263//yelp_interim//f1_user_index.txt")



// Load and parse the data
val data = sc.textFile("hdfs:///user//nvp263//yelp_interim//f1.txt")
val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

// Cluster the data into two classes using KMeans
val numClusters = 3
val numIterations = 20
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
println("Within Set Sum of Squared Errors = " + WSSSE)

// Save and load model
clusters.save(sc, "hdfs:///user//nvp263//yelp_interim//KMeansModel")

display(clusters, data)