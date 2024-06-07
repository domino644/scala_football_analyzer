package agh.scala.footballanalyzer
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
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


                val passCountAndAccuracyDf = passEventsDf.groupBy(
                        col("team.name").as("team_name") ,
                        col("player.name").as("player_name")
                    )
                    .agg(
                        count("*").as("total_passes"),
                        count(when(col("pass.outcome.name").isNull , 1)).as("accurate_passes")
                    ).withColumn("pass_accuracy",
                    round(col("accurate_passes") / col("total_passes") * 100))
                    .orderBy(col("team_name"), col("pass_accuracy").desc)


                passCountAndAccuracyDf.show(30, truncate = false)

            case None => println("DataFrame is not initialized")
        }

    }


    def get_exact_player_pass_information():Unit = {
        gameDF match {
            case Some(df) =>
                val passEventsDF = df.filter(col("type.name") === "Pass")

                val exactPassInfo = passEventsDF.groupBy(
                    col("team.name").as("team_name"),
                    col("player.name").as("player_name")
                )
                .agg(
                    count("*").as("total_passes"),
                    count(when(col("pass.outcome.name").isNull, 1)).as("accurate_passes"),
                    count(when(col("pass.outcome.name").isNull && col("pass.body_part.name") === "Drop Kick", 1)).as("accurate_drop_kick_passes"),
                    count(when(col("pass.body_part.name") === "Drop Kick", 1)).as("total_drop_kick_passes"),
                    count(when(col("pass.outcome.name").isNull && col("pass.body_part.name") === "Head", 1)).as("accurate_head_passes"),
                    count(when(col("pass.body_part.name") === "Head", 1)).as("total_head_passes"),
                    count(when(col("pass.outcome.name").isNull && col("pass.body_part.name") === "Keeper Arm", 1)).as("accurate_keeper_arm_passes"),
                    count(when(col("pass.body_part.name") === "Keeper Arm", 1)).as("total_keeper_arm_passes"),
                    count(when(col("pass.outcome.name").isNull && col("pass.body_part.name") === "Left Foot", 1)).as("accurate_left_foot_passes"),
                    count(when(col("pass.body_part.name") === "Left Foot", 1)).as("total_left_foot_passes"),
                    count(when(col("pass.outcome.name").isNull && col("pass.body_part.name") === "Right Foot", 1)).as("accurate_right_foot_passes"),
                    count(when(col("pass.body_part.name") === "Right Foot", 1)).as("total_right_foot_passes"),
                    count(when(col("pass.outcome.name").isNull && col("pass.body_part.name") === "Other", 1)).as("accurate_other_body_part_passes"),
                    count(when(col("pass.body_part.name") === "Other", 1)).as("total_other_body_part_passes"),
                    count(when(col("pass.outcome.name").isNull && col("pass.body_part.name") === "No Touch", 1)).as("accurate_no_touch_passes"),
                    count(when(col("pass.body_part.name") === "No Touch", 1)).as("total_no_touch_passes"),
                    count(when(col("pass.outcome.name").isNull && col("pass.type.name") === "Throw-in", 1)).as("accurate_throw_in_passes"),
                    count(when(col("pass.type.name") === "Throw-in", 1)).as("total_throw_in_passes"),
                    count(when(col("pass.outcome.name").isNull && col("pass.body_part.name").isNull && col("pass.type.name") === "Recovery", 1)).as("accurate_recovery_passes"),
                    count(when(col("pass.type.name") === "Recovery" && col("pass.body_part.name").isNull, 1)).as("total_recovery_passes")
                )
                    .withColumn("pass_accuracy", round(col("accurate_passes") / col("total_passes") * 100))
                    .withColumn("drop_kick_passes_accuracy", coalesce(round(col("accurate_drop_kick_passes") / col("total_drop_kick_passes") * 100), lit(0)))
                    .withColumn("head_passes_accuracy", coalesce(round(col("accurate_head_passes") / col("total_head_passes") * 100),lit(0)))
                    .withColumn("keeper_arm_passes_accuracy", coalesce(round(col("accurate_keeper_arm_passes") / col("total_keeper_arm_passes") * 100), lit(0)))
                    .withColumn("left_foot_passes_accuracy", coalesce(round(col("accurate_left_foot_passes") / col("total_left_foot_passes") * 100), lit(0)))
                    .withColumn("right_foot_passes_accuracy", coalesce(round(col("accurate_right_foot_passes") / col("total_right_foot_passes") * 100), lit(0)))
                    .withColumn("other_body_part_passes_accuracy", coalesce(round(col("accurate_other_body_part_passes") / col("total_other_body_part_passes") * 100), lit(0)))
                    .withColumn("no_touch_passes_accuracy", coalesce(round(col("accurate_no_touch_passes") / col("total_no_touch_passes") * 100), lit(0)))
                    .withColumn("throw_in_passes_accuracy", coalesce(round(col("accurate_throw_in_passes") / col("total_throw_in_passes") * 100), lit(0)))
                    .withColumn("recovery_passes_accuracy", coalesce(round(col("accurate_recovery_passes") / col("total_recovery_passes") * 100), lit(0)))

                    .select(
                        col("team_name"),
                        col("player_name"),
                        col("accurate_passes"),
                        col("total_passes"),
                        col("pass_accuracy"),
                        col("accurate_drop_kick_passes"),
                        col("total_drop_kick_passes"),
                        col("drop_kick_passes_accuracy"),
                        col("accurate_head_passes"),
                        col("total_head_passes"),
                        col("head_passes_accuracy"),
                        col("accurate_keeper_arm_passes"),
                        col("total_keeper_arm_passes"),
                        col("keeper_arm_passes_accuracy"),
                        col("accurate_left_foot_passes"),
                        col("total_left_foot_passes"),
                        col("left_foot_passes_accuracy"),
                        col("accurate_right_foot_passes"),
                        col("total_right_foot_passes"),
                        col("right_foot_passes_accuracy"),
                        col("accurate_other_body_part_passes"),
                        col("total_other_body_part_passes"),
                        col("other_body_part_passes_accuracy"),
                        col("accurate_no_touch_passes"),
                        col("total_no_touch_passes"),
                        col("no_touch_passes_accuracy"),
                        col("accurate_throw_in_passes"),
                        col("total_throw_in_passes"),
                        col("throw_in_passes_accuracy"),
                        col("accurate_recovery_passes"),
                        col("total_recovery_passes"),
                        col("recovery_passes_accuracy")
                    )

                    .orderBy(col("team_name"), col("pass_accuracy").desc)

                exactPassInfo.show(30, truncate = false)

            case None => println("DataFrame is not initialized")


        }
    }


    def get_player_shots_number_and_accuracy():Unit = {
        gameDF match{
            case Some(df) =>
                val shotsEventsDF = df.filter(col("type.name") === "Shot")


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

    def get_player_total_time_whit_ball():Unit ={
        gameDF match {
            case Some(df) =>
                val carryEventsDF = df.filter(col("type.name") === "Carry")

                val ballCarryTimeDF = carryEventsDF.groupBy(
                    col("team.name").as("team_name"),
                    col("player.name").as("player_name")
                )
                .agg(
                    round(sum(col("duration"))/60, 2).as("total_time_with_ball")
                )
                .orderBy(col("team_name"), col("total_time_with_ball").desc)

                ballCarryTimeDF.show(30, truncate = false)

            case None => println("DataFrame is not initialized")
        }

    }


    def get_player_dribble_number_and_win_ratio():Unit= {
        gameDF match  {
            case Some(df) =>
                val dribbleEventsDF = df.filter(col("type.name") === "Dribble")

                val dribbleCountAndRatio = dribbleEventsDF.groupBy(
                    col("team.name").as("team_name"),
                    col("player.name").as("player_name")
                )
                .agg(

                    count(when(col("dribble.outcome.name") === "Complete", 1)).as("successful_dribbles"),
                    count("*").as("total_dribbles"),

                )
                .withColumn("dribbles_win_ratio", round(col("successful_dribbles") / col("total_dribbles")*100))
                .orderBy(col("team_name"), col("dribbles_win_ratio").desc)

                dribbleCountAndRatio.show(30, truncate = false)

            case None => println("DataFrame is not initialized")

        }
    }


    def get_player_ball_recovery_number_and_ratio():Unit = {
        gameDF match {
            case Some(df) =>
                val recoveriesEventsDF = df.filter(col("type.name") === "Ball Recovery")

                val recoveriesCountAndRatio = recoveriesEventsDF.groupBy(
                    col("team.name").as("team_name"),
                    col("player.name").as("player_name")
                )
                .agg(
                    count(when(col("ball_recovery.recovery_failure").isNull, 1)).as("successful_ball_recoveries"),
                    count("*").as("total_ball_recoveries")
                )
                .withColumn("ball_recovery_ratio", round(col("successful_ball_recoveries")/col("total_ball_recoveries")*100))
                .orderBy(col("team_name"), col("ball_recovery_ratio").desc)

                recoveriesCountAndRatio.show(30, truncate = false)


            case None => println("DataFrame is not initialized")
        }
    }

    def get_player_block_count_and_ratio(): Unit = {
        gameDF match {
            case Some(df) =>
                val blocksEventsDF = df.filter(col("type.name") === "Block")



                def nestedFieldExists(columnName: String): Boolean = {
                    blocksEventsDF.schema.find(_.name == "block") match {
                        case Some(StructField(_, ,StructType(fields), _, _)) =>
                            fields.exists(_.name == columnName)
                        case _ => false
                    }
                }

                val deflectionBlockExists = nestedFieldExists("deflection")
                val offensiveBlockExists = nestedFieldExists("offensive")
                val saveBlockExists = nestedFieldExists("save_block")
                val counterpressBlockExists = nestedFieldExists("counterpress")


                var aggExprs = Seq(
                    count("*").as("total_blocks"),
                    count(when(col("block").isNull, 1)).as("standard_blocks")
                )


                if (deflectionBlockExists) {
                    aggExprs :+= count(when(col("block.deflection").isNotNull, 1)).as("deflection_blocks")
                }
                if (offensiveBlockExists) {
                    aggExprs :+= count(when(col("block.offensive").isNotNull, 1)).as("offensive_blocks")
                }
                if (saveBlockExists) {
                    aggExprs :+= count(when(col("block.save_block").isNotNull, 1)).as("save_blocks")
                }
                if (counterpressBlockExists) {
                    aggExprs :+= count(when(col("block.counterpress").isNotNull, 1)).as("counterpress_blocks")
                }


                val blockPerTypeCount = blocksEventsDF.groupBy(
                    col("team.name").as("team_name"),
                    col("player.name").as("player_name")
                ).agg(aggExprs.head, aggExprs.tail: _*)


                var withRatios = blockPerTypeCount
                    .withColumn("standard_block_ratio", round(col("standard_blocks") / col("total_blocks") * 100))


                if (deflectionBlockExists) {
                    withRatios = withRatios.withColumn("deflection_block_ratio", round(col("deflection_blocks") / col("total_blocks") * 100))
                }
                if (offensiveBlockExists) {
                    withRatios = withRatios.withColumn("offensive_block_ratio", round(col("offensive_blocks") / col("total_blocks") * 100))
                }
                if (saveBlockExists) {
                    withRatios = withRatios.withColumn("save_block_ratio", round(col("save_blocks") / col("total_blocks") * 100))
                }
                if (counterpressBlockExists) {
                    withRatios = withRatios.withColumn("counterpress_block_ratio", round(col("counterpress_blocks") / col("total_blocks") * 100))
                }

                val selectedColumns = Seq(
                    col("team_name"),
                    col("player_name"),
                    col("total_blocks"),
                    col("standard_blocks"),
                    col("standard_block_ratio")
                ) ++ (
                    if (deflectionBlockExists) Seq(col("deflection_blocks"), col("deflection_block_ratio")) else Seq()
                    ) ++ (
                    if (offensiveBlockExists) Seq(col("offensive_blocks"), col("offensive_block_ratio")) else Seq()
                    ) ++ (
                    if (saveBlockExists) Seq(col("save_blocks"), col("save_block_ratio")) else Seq()
                    ) ++ (
                    if (counterpressBlockExists) Seq(col("counterpress_blocks"), col("counterpress_block_ratio")) else Seq()
                    )

                val finalDF = withRatios.select(selectedColumns: _*)
                    .orderBy(col("team_name"), col("total_blocks").desc)

                finalDF.show(30, truncate = false)

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
        analyzer.get_player_block_count_and_ratio()
        analyzer.get_player_ball_recovery_number_and_ratio()
        analyzer.get_player_dribble_number_and_win_ratio()
        analyzer.get_player_total_time_whit_ball()
        analyzer.get_player_pass_number_and_accuracy()
        analyzer.get_exact_player_pass_information()
        analyzer.get_player_shots_number_and_accuracy()
        analyzer.get_all_substitution()
        analyzer.getPlayersWithNumbersAndPositions()
        spark.stop()
    }
}