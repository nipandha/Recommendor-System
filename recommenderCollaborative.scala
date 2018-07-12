sc.setLogLevel("ERROR")
import scala.util.parsing.json.JSON
import org.apache.spark.HashPartitioner
import sqlContext.implicits._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

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
show_business_string(fields.get("stars")).toDouble)).zipWithIndex

val businesses_filtered = businesses.filter(_._1._4=="AZ")
val business=businesses_filtered.keyBy(_._2).mapValues(_._1).partitionBy(new HashPartitioner(10)).persist()
val b_rev = businesses.keyBy(_._1._1).mapValues(_._2).partitionBy(new HashPartitioner(10)).persist()
val business_rev_indexed = b_rev.collectAsMap
def return_business_index(bid: String) = {
if (business_rev_indexed.exists(_._1 == bid)) 
{business_rev_indexed(bid)	}	
else {-1.0}
}
def show(x: Option[String]) = x match {
case None => "?" 
case _ => x.toString.drop(5).dropRight(1)                    
}


val users_unparsed = sc.textFile("hdfs:///user//nvp263//yelp//user.json")
val users_parsed = users_unparsed.map(line => JSON.parseFull(line).get.asInstanceOf[Map[String,String]])
val users = users_parsed.take.map(fields=>(show(fields.get("user_id")),show(fields.get("name")),show(fields.get("review_count")).toDouble.toInt,
 - show(fields.get("yelping_since")).dropRight(6).toDouble.toInt, 
show(fields.get("average_stars")).toDouble)).zipWithIndex
val users_indexed=users.keyBy(_._2).mapValues(_._1).partitionBy(new HashPartitioner(10)).persist()
val u_rev = users.keyBy(_._1._1).mapValues(_._2).partitionBy(new HashPartitioner(10)).persist()
val users_rev_indexed = u_rev.collectAsMap
def return_user_index(uid: String) = {
if (users_rev_indexed.exists(_._1 == uid)) 
{users_rev_indexed(uid)	}	
else {-1.0}
}

val reviews_unparsed = sc.textFile("hdfs:///user//nvp263//yelp//review.json")
val reviews_parsed = reviews_unparsed.map(line => JSON.parseFull(line).get.asInstanceOf[Map[String,String]])
val reviews=reviews_parsed.map(fields=>(show(fields.get("review_id")),
return_user_index(show(fields.get("user_id")).toString),return_business_index(business_rev_indexed(show(fields.get("business_id"))).toString),
show(fields.get("text")),show(fields.get("stars")).toDouble)).zipWithIndex
val reviews_filtered = reviews.filter(_._1._2>-1.0)
val reviews = reviews_filtered.filter(_._1._3>-1.0)
val r1 = reviews.map(fields=> (fields._2,fields._1._1,fields._1._2,fields._1._3,fields._1._5))
val reviews_b_indexed=r1.keyBy(_._4.toLong)
val reviews_to_consider = business.join(reviews_b_indexed).collect
val n = business.count()

var user_vectors = collection.mutable.Map[Double,Array[Double]]()
def add_vector(uid:Double , bid: Double , rating:Double ) = {
var vec = Array.fill(n.toInt)(0.0)
if(user_vectors.exists(_._1 == uid)) {
vec = user_vectors(uid)
vec(bid.toInt) = rating
user_vectors(uid.toInt) = vec
}
else {
vec(bid.toInt) = rating
user_vectors += (uid->vec)
}
}
reviews_to_consider.map(fields => add_vector(fields._2._2._3,fields._2._2._4,fields._2._2._5))
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

val sqlContext = new SQLContext(sc) 
val data = vectors.map(_._2).toDF()
val features = data.map(_.getAs[Vector]("features"))
val clusters = KMeans.train(features, 3, 10)
display(clusters, data)