package io.saagie.esgi.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc


// avant de run, mettez les deux variables dans votre config
// 30000 local

object SparkFinalProject {


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

    val beers_with_reviews = beers
      .join(reviews, beers("id") ===  reviews("beer_id"), "inner")
//      .select(beers_with_reviews."beer_id")
    //.orderBy(desc("score"))
    //.limit(5)

    beers_with_reviews.show()

//    df_baby_names.groupBy("FirstName").sum("Count")
//      .orderBy($"sum(Count)".desc)
//      .limit(5)
//      .show()

    //beers_with_reviews.show()


    //.saveAsTextFile(path_file + "/results.csv")

    println(s"Sleeping for ${args(0)}")

    Thread.sleep(args(0).toLong)
  }

}
