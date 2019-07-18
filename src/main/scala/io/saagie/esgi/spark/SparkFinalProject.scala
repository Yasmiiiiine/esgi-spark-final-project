package io.saagie.esgi.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, desc, lower, sum, avg}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.desc


// avant de run, mettez les deux variables dans votre config
// 30000 local

object SparkFinalProject {

  case class Reviews(beer_id: Int, score: String)
  case class Beers(brewery_id: String, id: Int)
  case class Breweries(id: String, name: String)

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    var path_file = "";

    // args(1) variable qui dit si on execute en local ou sur saagie (avec le hdfs du prof)
    if (args(1) == "local") {
      path_file += "data"
    } else {
      path_file += "hdfs:/data/esgi-spark/final-project"
    }
    val beers = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path_file + "/beers.csv")
    //beers.show()

    val reviews = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path_file + "/reviews.csv")
    //reviews.show()

    val breweries = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path_file + "/breweries.csv")
    //breweries.show()

    val stopwords = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .textFile(path_file + "/stopwords.txt")
    //reviews.show()


    beers.createOrReplaceTempView("beers")
    reviews.createOrReplaceTempView("reviews")
    breweries.createOrReplaceTempView("breweries")

    val rev: Dataset[Reviews] = reviews.as[Reviews]
    val bee: Dataset[Beers] = beers.as[Beers]
    val bre: Dataset[Breweries] = breweries.as[Breweries]

    val beers_with_reviews = beers
      .join(reviews, beers("id") ===  reviews("beer_id"), "inner")


    // Question 1
    println("Question 1 : La bière qui a le meilleur score (moyenne des notes de chaque client)")
    val beer_with_best_score = beers_with_reviews
      .filter($"score".between(0,5))
      .orderBy($"score".desc)
      .limit(1)
      .show()

    // Question 2
    println("Question 2 : La brasserie qui a le meilleur score en moyenne sur toutes ses bières")
    val rev_rename =
      rev
        .select("beer_id", "score")
        .withColumnRenamed("beer_id", "rev_beer_id")
        .withColumnRenamed("score", "beer_score")

    val jointure_beer_reviews =
      rev_rename
        .join(bee, rev_rename("rev_beer_id") === bee("id"), "inner")
        .select("rev_beer_id", "beer_score", "brewery_id")

    val best_brewery =
      jointure_beer_reviews
        .join(bre, bre("id") === jointure_beer_reviews("brewery_id"), "inner")
        .groupBy("brewery_id", "name")
        .agg(avg("beer_score").as("Nb"))
        .orderBy(desc("Nb"))
        .limit(1)
        .show()


    // Question 3
    println("Question 3 : Les 10 pays qui ont le plus de brasseries de type Beer-to-go")
    bre
      .filter(bre("types") contains "Beer-to-go")
      .groupBy("country")
      .count()
      .orderBy(desc("count"))
      .limit(10)
      .show()

    // Question 4
    println("Question 4 : le mot qui revient le plus (avec le plus d'occurrences) " +
      "dans les reviews concernant les bières de style IPA")
//    val frequent_word =
    val textDataset = reviews
      .join(beers, beers("id") === reviews("beer_id"), "left")
      .filter(beers("style").contains("IPA"))
      .select(reviews("text"))
      .as[String]


    val stopwords2 = stopwords.withColumnRenamed("value", "word")

    val top10Words = textDataset
      .flatMap(_.toLowerCase
        .replaceAll("""[\p{Punct}&&[^.]]""", "")
        .trim
        .replaceAll(" +", " ")
        .replaceAll("  ", " ")
        .split(" ")
      )
      .groupBy("Value")
      .count()
      .orderBy($"count".desc)
      .join(stopwords2, $"Value" === stopwords2("word"), "left_anti")
      .filter( $"Value" rlike "[{IsLetter}]")
      .show(1)
//      stopwords2.show()
    println(s"Sleeping for ${args(0)}")

    Thread.sleep(args(0).toLong)
  }

}
