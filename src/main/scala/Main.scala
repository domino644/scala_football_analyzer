package agh.scala.footballanalyzer
import classes.FootballAnalyzer
import objects.SparkSessionSingleton


object Main {
    def main(args: Array[String]): Unit = {
        val spark = SparkSessionSingleton.createOrGetInstance
        val url = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/15946.json"
        val analyzer = new FootballAnalyzer(spark)
        analyzer.initializeDataFrame(url)
        analyzer.getPlayerFoulsWon() match {
            case Some(df) => df.show()
            case _ =>
        }
        spark.stop()
    }
}