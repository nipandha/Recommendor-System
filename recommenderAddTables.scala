sc.setLogLevel("ERROR")
import scala.util.parsing.json.JSON
import org.apache.spark.HashPartitioner


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

val businesses_filtered = businesses.filter(_._4=="AZ")
val business=businesses.keyBy(_._2).mapValues(_._1).partitionBy(new HashPartitioner(10)).persist()
val business_rev_indexed = businesses.keyBy(_._1._1).mapValues(_._2).partitionBy(new HashPartitioner(10)).persist()
def show(x: Option[String]) = x match {
case None => "?" 
case _ => x.toString.drop(5).dropRight(1)                    
}

val users_unparsed = sc.textFile("hdfs:///user//nvp263//yelp//user.json")
val users_parsed = users_unparsed.map(line => JSON.parseFull(line).get.asInstanceOf[Map[String,String]])
val users = users_parsed.map(fields=>(show(fields.get("user_id")),show(fields.get("name")),show(fields.get("review_count")).toDouble.toInt,
 - show(fields.get("yelping_since")).dropRight(6).toDouble.toInt, 
show(fields.get("average_stars")).toDouble)).zipWithIndex
val users_indexed=users.keyBy(_._2).mapValues(_._1).partitionBy(new HashPartitioner(10)).persist()
val users_rev_indexed = users.keyBy(_._1._1).mapValues(_._2).partitionBy(new HashPartitioner(10)).persist()

val reviews_unparsed = sc.textFile("hdfs:///user//nvp263//yelp//review.json")
val reviews_parsed = reviews_unparsed.map(line => JSON.parseFull(line).get.asInstanceOf[Map[String,String]])
val reviews=reviews_parsed.map(fields=>(show(fields.get("review_id")),
users_rev_indexed.lookup(show(fields.get("user_id"))),business_rev_indexed.lookup(show(fields.get("business_id"))),
show(fields.get("text")),show(fields.get("stars")).toDouble)).zipWithIndex