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
	show_business_string(fields.get("stars")).toDouble))

val business=businesses.keyBy(fields=>fields._1).partitionBy(new HashPartitioner(100)).persist()
def find_no_common_elements(l1: List[String], l2: List[String]) = {
	l1.toSet.intersect(l2.toSet).size
} 

def find_top_k_similar(Int k) = {
	val max = Array.fill(k)(0)

}
def find_similar_business_ids(String bid) = {
	val l = businesses.filter(fields=>fields._1==bid).take(1)(0)._5
	val business_similar_categories = businesses.map(fields=>(fields._1,find_no_common_elements(l,fields._5)))
	val similar_businesses_filtered = business_similar_categories.filter(_._2>1)
	val most_similar_businesses = similar_businesses_filtered.sortBy(_._2,false)
	return most_similar_businesses.take(10).map(_._1)
}

