package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
  //addLine
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WikiSparkApp")
  //addLine
  val sc: SparkContext = new SparkContext(conf)
  //addLine
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(line => WikipediaData.parse(line)).persist()

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  //addLine
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    //aggregate(0)(_+_, _+_)
    //The first _+_ is the intra-partition sum, adding the total number of elements picked by each picker
    //The second _+_ is the inter-partition sum, which aggregates the total sums from each quadrant
    rdd.aggregate(0)(
      (count, article) => article.mentionsLanguage(lang) match{
        case true => count + 1
        case _ => count + 0
      },
      (a, b) => a + b
    )

}

/* (1) Use `occurrencesOfLang` to compute the ranking of the languages
 *     (`val langs`) by determining the number of Wikipedia articles that
 *     mention each language at least once. Don't forget to sort the
 *     languages by their occurrence, in decreasing order!
 *
 *   Note: this operation is long-running. It can potentially run for
 *   several seconds.
 */
  //addLine
def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
  langs.map(language => (language, occurrencesOfLang(language,rdd))).sortBy(par => par._2)(Ordering[Int].reverse)
}

/* Compute an inverted index of the set of articles, mapping each language
 * to the Wikipedia pages in which it occurs.
 */
  //addLine
def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    rdd.flatMap(article => langs.map(
      lang => article.mentionsLanguage(lang) match {
        case true => (lang, article)
        case false => null })
    ).filter(_ != null).groupByKey()//first Argument
  }

/* (2) Compute the language ranking again, but now using the inverted index. Can you notice
 *     a performance improvement?
 *
 *   Note: this operation is long-running. It can potentially run for
 *   several seconds.
 */
def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
  index.mapValues(articles => articles.count(_ => true))
    .collect().toList.filter(_._2 != 0).sortBy(_._2).reverse
}

/* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
 *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
 *     and the computation of the ranking? If so, can you think of a reason?
 *
 *   Note: this operation is long-running. It can potentially run for
 *   several seconds.
 */
def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
  rdd.flatMap(a => langs.map(lang => (lang, a.mentionsLanguage(lang) match {
    case true => 1
    case false => 0 })))
    .reduceByKey(_ + _).sortBy(pair => pair._2, false).collect().toList
}

def main(args: Array[String]) {

  /* Languages ranked according to (1) */
  val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

  /* An inverted index mapping languages to wikipedia pages on which they appear */
  def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

  /* Languages ranked according to (2), using the inverted index */
  val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

  /* Languages ranked according to (3) */
  val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

  /* Output the speed of each ranking */
  println(timing)
  sc.stop()
}

val timing = new StringBuffer
def timed[T](label: String, code: => T): T = {
  val start = System.currentTimeMillis()
  val result = code
  val stop = System.currentTimeMillis()
  timing.append(s"Processing $label took ${stop - start} ms.\n")
  result
}
}
