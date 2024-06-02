package agh.scala.footballanalyzer
import org.apache.spark.sql
import org.apache.spark.sql.functions.{collect_list, concat_ws, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Try, Using}
import scala.io.Source

object SparkSessionSingleton{
    @transient private var instance: SparkSession = _

    def getInstance: SparkSession = {
        println("Instance:"+instance)
        if(instance == null){
            instance = SparkSession.builder()
                .appName("football_analyzer")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate()
        }
        instance
    }
}

class FootballAnalyzer (
    val spark: SparkSession,
    val url:String,
    ){
    import spark.implicits._
    private var gameDF: Option[DataFrame] = None


    def showDF():Unit = {
        gameDF.foreach(_.show())

    }

    def getPlayersWithNumbersAndPositions():Unit = {

        gameDF match{
            case Some(df) =>
                val selectedDF = df.select($"team.name".as("Team name"), explode($"tactics.lineup").as("lineup"))

                val playerDF = selectedDF.select(
                    $"Team name", $"lineup.player.name".as("player_name"),
                    $"lineup.position.name".as("player_position"),
                    $"lineup.jersey_number".as("player_number")
                ).distinct()

                val groupedDF = playerDF
                    .groupBy($"Team name", $"player_name", $"player_number")
                    .agg(collect_list($"player_position").as("player_positions"))
                    .orderBy($"Team name", $"player_number")

                val formattedDF = groupedDF.withColumn("player_positions", concat_ws(", ", $"player_positions"))

                formattedDF.show(30, truncate = false)

            case None => println("DataFrame is not initialized")

        }


    }


    def initializeDataFrame():Unit = {
        gameDF = Some(getDFFromUrl)
    }

    private def getDFFromUrl:sql.DataFrame = {
        val json: Try[String]= Using(Source.fromURL(url)) {source => source.mkString}

        json match {
            case scala.util.Success(content) =>

                 spark.read.json(Seq(content).toDS)


            case scala.util.Failure(exception) => println(s"Error occurred: ${exception.getMessage}")
            throw exception
        }
    }
}



object Main {
    def main(args: Array[String]): Unit = {
        val spark = SparkSessionSingleton.getInstance
        val url = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/15946.json"
        val analyzer = new FootballAnalyzer(spark, url)
        analyzer.initializeDataFrame()
        analyzer.showDF()
        analyzer.getPlayersWithNumbersAndPositions()
        spark.stop()
    }
}