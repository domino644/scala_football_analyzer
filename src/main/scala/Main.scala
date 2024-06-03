package agh.scala.footballanalyzer
import org.apache.spark.sql
import org.apache.spark.sql.functions._
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

    def get_all_substitution():Unit = {
        gameDF match{
            case Some(df) =>
                val selectedDF = df.select(
                    $"team.name".as("Team_name"),
                    $"player.name".as("player_out"),
                    $"substitution.replacement.name".as("player_in"),
                    $"minute".as("min"),
                    $"second".as("sec"),
                    $"position.name".as("position"),
                    $"substitution.outcome.name".as("substitution_reason"),


                ).where("Substitution is not NULL")



                val timeFormattedDF = selectedDF
                    .withColumn(
                        "substitution_time", expr("lpad(min, 2, '0') || ':' || lpad(sec, 2, '0')")
                    ).drop($"min", $"sec")


                val sortedDF = timeFormattedDF.orderBy($"Team_name", $"substitution_time")

                sortedDF.show(truncate = false)

            case None => println("DataFrame is not initialized")
        }
    }


    def get_player_pass_number_and_accuracy():Unit = {
        gameDF match {
            case Some(df) =>

                val passEventsDf = df.filter(col("type.name") === "Pass")
                passEventsDf.show()

                val passCountAndAccuracyDf = passEventsDf.groupBy(
                        col("team.name").as("team_name") ,
                        col("player.name").as("player_name")
                    )
                    .agg(
                        count("*").as("total_passes"),
                        count(when(col("pass.outcome.name").isNull, 1)).as("accurate_passes")
                    ).withColumn("pass_accuracy",
                    round(col("accurate_passes") / col("total_passes") * 100))
                    .orderBy(col("team_name"), col("pass_accuracy").desc)


                passCountAndAccuracyDf.show(30, truncate = false)

            case None => println("DataFrame is not initialized")
        }

    }


    def get_player_shots_number_and_accuracy():Unit = {
        gameDF match{
            case Some(df) =>
                val shotsEventsDF = df.filter(col("type.name") === "Shot")
                shotsEventsDF.show()

                val shotCountAndAccuracyDf = shotsEventsDF.groupBy(
                    col("team.name").as("team_name"),
                    col("player.name").as("player_name")
                )
                    .agg(
                        count("*").as("total_shots"),
                        count(when(col("shot.outcome.name") === "Goal", 1)).as("goal"),
                        count(when(col("shot.outcome.name").isin("Saved","Saved off T", "Saved to Post", "Blocked"), 1)).as("blocked_or_saved_by_goalkeeper"),
                        count(when(col("shot.outcome.name").isin("Off T", "Post", "Wayward"), 1)).as("not_on_target")
                    )
                    .withColumn("on_target_accuracy", round((col("total_shots") - col("not_on_target")) / col("total_shots") * 100))
                    .withColumn("not_on_target_accuracy", round(col("not_on_target") / col("total_shots") * 100))
                    .withColumn("shots_per_goal_ratio", round(col("goal") / col("total_shots") * 100))
                    .orderBy(col("team_name"), col("on_target_accuracy").desc)

                shotCountAndAccuracyDf.show(truncate = false)

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
        analyzer.get_player_pass_number_and_accuracy()
        analyzer.get_player_shots_number_and_accuracy()
        analyzer.get_all_substitution()
        analyzer.getPlayersWithNumbersAndPositions()
        spark.stop()
    }
}