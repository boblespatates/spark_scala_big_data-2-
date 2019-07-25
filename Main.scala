package elk_package


object Main {

  def main(args: Array[String]): Unit = {

    //////////////////// LOCAL USE //////////////////////////

    /*
    // Initialisation of the file path for : local use
    val filepathAnimeList = "C:\\Users\\nzablocki\\Documents\\senarioBigData\\AnimeList.csv"
    //val filepathUserAnimeList = "C:\\Users\\nzablocki\\Documents\\senarioBigData\\UserAnimeList.csv"
    val filepathUserAnimeList = "C:\\Users\\nzablocki\\Documents\\senarioBigData\\UserAnimeList_light.csv"
    val filepathUserList = "C:\\Users\\nzablocki\\Documents\\senarioBigData\\UserList.csv"
     */


    ////////////////////// SANDBOX USE //////////////////////

    // Initialisation of the file path for : HDP
    val filepathAnimeList = "hdfs:///tmp/data/scenarioBigData/AnimeList.csv"
    //val filepathUserAnimeList = "hdfs:///tmp/data/UserAnimeList.csv"
    val filepathUserAnimeList = "hdfs:///tmp/data/scenarioBigData/UserAnimeList_light.csv"
    val filepathUserList = "hdfs:///tmp/data/scenarioBigData/UserList.csv"


    // Intialisation Of the Exercice dealer which will deal with the different part od the exercice
    val exercicePartDealer = new ExercicePartDealer(filepathAnimeList,
      filepathUserAnimeList,
      filepathUserList)
  }
}
