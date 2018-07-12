import scala.collection.mutable
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import scala.util.parsing.json.JSON

// parse the reviews JSON file
val reviewsJSON = sc.textFile("recommenderSystem/reviews_Books_5.json")
val reviewsMap = reviewsJSON.map(line => JSON.parseFull(line).get.asInstanceOf[Map[String, String]])
// obtain an RDD with reviewText
val reviewsRDD = reviewsMap.map(line => line.values.toList)
val reviewText = reviewsRDD.map(line => line(3))

// split each review into a sequence of words
val reviewWords: RDD[Seq[String]] =
  reviewText.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))

//   wordCount: sorted list of (word, wordCount) pairs
val wordCount: Array[(String, Long)] =
  reviewWords.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
//   wordVector: vector of words - with stopwords removed
val numStopwords = 20
val wordVector: Array[String] =
  wordCount.takeRight(wordCount.size - numStopwords).map(_._1)
//   words: map each word -> word index
val words: Map[String, Int] = wordVector.zipWithIndex.toMap

// convert reviews into word count vectors
val reviews: RDD[(Long, Vector)] =
  reviewWords.zipWithIndex.map { case (tokens, id) =>
    val counts = new mutable.HashMap[Int, Double]()
    tokens.foreach { term =>
      if (words.contains(term)) {
        val idx = words(term)
        counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
      }
    }
    (id, Vectors.sparse(words.size, counts.toSeq))
  }

// set the LDA parameters
val numTopics = 10
val lda = new LDA().setK(numTopics).setMaxIterations(10)
val ldaModel = lda.run(reviews)
val avgLogLikelihood = ldaModel.logLikelihood / reviews.count()

// print the topics, showing the top-weighted 10 words for each topic.
val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
topicIndices.foreach { case (terms, termWeights) =>
  println("TOPIC:")
  terms.zip(termWeights).foreach { case (term, weight) =>
    println(s"${vocabArray(term.toInt)}\t$weight")
  }
  println()
}