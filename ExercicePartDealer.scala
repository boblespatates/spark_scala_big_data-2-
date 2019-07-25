package elk_package

import org.apache.spark.sql.DataFrame

class ExercicePartDealer(filepathAnimeList: String,
                         filepathUserAnimeList : String,
                         filepathUserList : String) {

  /////////////// ATTRIBUTES ///////////////////////

  // Intialisation of the spark ELK Object
  val sparkELKPart = new SparkELKPart(filepathAnimeList,
    filepathUserAnimeList,
    filepathUserList)

  val dfAnimeList = sparkELKPart.dfAnimeList
  val dfUserAnimeList = sparkELKPart.dfUserAnimeList
  val dfUserList = sparkELKPart.dfUserList


  ////////////// METHODS ///////////////////////////

  // Display the result of the methods defined to answer the  ELK senario
  def sparkPartShow(sparkELKPart: SparkELKPart) : Unit = {

    // DataFrame initialisation
    val dfAnimeList = sparkELKPart.dfAnimeList
    val dfUserAnimeList = sparkELKPart.dfUserAnimeList
    val dfUserList = sparkELKPart.dfUserList

    //sparkELKPart.animeByUser(dfUserAnimeList).show // question 2
    //sparkELKPart.mostWatchedAnimeByUser(dfUserAnimeList).show // question 3
    //sparkELKPart.mostWatchedEpisodByAnime(dfAnimeList).show // question 4
    //sparkELKPart.userWatchedMostAnime(dfUserAnimeList).show // question 5
    //sparkELKPart.userWatchedMostEpisod(dfUserList).show // question 6
    //sparkELKPart.nbUsers(dfUserList).show // question 7
    //sparkELKPart.userBetterMean(dfUserList).show  // question 8
    var dfAllMerged = sparkELKPart.filesjoin(dfUserList, dfUserAnimeList, dfAnimeList).persist() // question 9
    //dfAllMerged.show // question 9

    //////////// IN PROGRESS ////////////
    //sparkELKPart.save2Hive(dfAllMerged, "AllMerged", "bigdata_db")


  }
