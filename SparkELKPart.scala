package elk_package

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
//import com.hortonworks.hwc.HiveWarehouseSession


class SparkELKPart(filepathAnimeList : String,
                      filepathUserAnimeList : String,
                      filepathUserList : String) {

  ////////////// ATTRIBUTES /////////////////

  val spark = SparkSession.builder()
    .appName("Senario Big Data")
    .config("spark.master", "local")
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .enableHiveSupport()
    .getOrCreate()

  //spark.conf.get("spark.sql.warehouse.dir")// Output: res2: String = /apps/spark/warehouse
  //spark.conf.get("hive.metastore.warehouse.dir")// NotSuchElement Exception
  //spark.conf.get("spark.hadoop.hive.metastore.uris")// NotSuchElement Exception

  // This import enable us to call $"column_name"
  import spark.implicits._

  // Initialisation of the DataFrame AnimeList
  val dfAnimeList = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filepathAnimeList)


  // Initialisation of the DataFrame UserAnimeList
  val dfUserAnimeList = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filepathUserAnimeList)


  // Initialisation of the DataFrame UserList
  val dfUserList = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filepathUserList)


  //////////// METHOD DEFINITION /////////


  // Number of Anime watched by Users : question 2
  def animeByUser(dfUserAnimeList : DataFrame) : DataFrame = {
    dfUserAnimeList.select("username", "anime_id")
      .groupBy("username")
      .agg(countDistinct("anime_id"))
  }


  // The most watched Anime by User : question 3
  def mostWatchedAnimeByUser(dfUserAnimeList : DataFrame) : DataFrame = {

    // Fill the empty cells : null -> 0 (null become 0)
    val dfNoNull = dfUserAnimeList.na.fill(0, Seq("my_watched_episodes", "my_rewatching", "my_rewatching_ep"))

    // Compute the number of episode watched by Anime and by User
    val dfTotalEp = dfNoNull.withColumn("total_ep_watched",
      $"my_watched_episodes"*
        $"my_rewatching"+
        $"my_rewatching_ep"+
        $"my_watched_episodes")

    // For each user take the most watched Anime, i.e the number max of episodes watched
    val byUsername = Window.partitionBy('username)
    val dfMaxEp = dfTotalEp.withColumn("max_total_ep_watched", max('total_ep_watched) over byUsername)
    dfMaxEp.select($"username", $"anime_id", $"max_total_ep_watched").where($"total_ep_watched" === $"max_total_ep_watched")
  }


  // TODO : Not possible to answer the question
  // Anime with the best score : question 4
  def mostWatchedEpisodByAnime(dfAnimeList : DataFrame) : DataFrame = {
    dfAnimeList
    /* // If we had the info we could rank to know most watched episod by users
    val byAnime = Window.partitionBy('anime_id).orderBy('my_watched_episodes desc)
    val rankByAnime = rank().over(byAnime)
    dfUserList.select('*, rankByAnime as 'rank).where('rank <= 1)
     */
  }


  // User who watched most number of anime : question 5
  def userWatchedMostAnime(dfUserAnimeList : DataFrame) : DataFrame = {

    dfUserAnimeList.select("username", "anime_id")
      .groupBy("username")
      .agg(countDistinct("anime_id").as("nb_anime_watched"))
      .orderBy(desc("nb_anime_watched"))
  }


  // User who watched most number of Episode : question 6
  def userWatchedMostEpisod(dfUserList : DataFrame) : DataFrame = {
    val maxValue = dfUserList.select(max($"stats_episodes")).head(1)(0)(0)
    dfUserList.select($"username").where($"stats_episodes" === maxValue)
  }


  // How many different users is there : question 7
  def nbUsers(dfUserList : DataFrame) : DataFrame = {
    dfUserList.select(countDistinct("user_id"))
  }


  // Select the 100 which give the better mean grade : question 8
  def userBetterMean(dfUserList : DataFrame): DataFrame = {
    dfUserList.select("username", "stats_mean_score")
      .orderBy(desc("stats_mean_score"))
      .limit(100)

    // A more fair way to do so is : But you need shuffling here
    /*
    val byUsername = Window.orderBy('stats_mean_score desc)
    val rankByUsername = rank().over(byUsername)
    dfUserList.select('*, rankByUsername as 'rank).where('rank <= 100)
     */
  }


  // Join the data frames : question 9
  def filesjoin(dfUserList : DataFrame,
                dfUserAnimeList : DataFrame,
                dfAnimeList : DataFrame) : DataFrame = {

    val cols = List("username", "gender", "location",
      "birth_date", "anime_id", "title_english", "duration",
      "rating", "score", "rank", "my_watched_episodes",
      "my_score", "my_status", "my_tags")


    val dfTmp = dfUserAnimeList.join(broadcast(dfUserList), "username")
    dfTmp.join(broadcast(dfAnimeList), "anime_id")
      .select(cols.head, cols.tail: _*)

    /* // If you only have one node
    dfAnimeList.join(dfUserAnimeList, Seq("anime_id"))
      .join(dfUserList, Seq("username"))
      .select(cols.head, cols.tail: _*)

     */
  }

  // Save a dataFrame into the spark
  def save2Hive(df :DataFrame, tableName : String, dbName : String): Unit = {

    spark.sql("CREATE DATABASE IF NOT EXISTS " + dbName + " LOCATION hdfs:///tmp/")
    spark.sql("show databases").show()
    spark.sql("show tables").show()

    //df.write.mode("overwrite").saveAsTable(dataBase + tableName)
    //df.write.mode("overwrite").saveAsTable("spark_db.AllMerged")
    //spark.sql("select * from " + dataBase + tableName).show()

  }

}
