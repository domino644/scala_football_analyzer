package agh.scala.footballanalyzer
package classes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, coalesce, col, collect_list, concat_ws, count, explode, expr, lit, round, sum, when}
import org.apache.spark.sql.types.{StructField, StructType}

class FootballAnalyzer(val spark: SparkSession) {

  import spark.implicits._

  private var gameDF: DataFrame = spark.emptyDataFrame

  def setGameDF(gameDF: DataFrame): Unit = {
    this.gameDF = gameDF
  }

  private def nestedFieldExists(dfSchema: StructType, parentField: String, nestedField: String): Boolean = {
    dfSchema.find(_.name == parentField) match {
      case Some(StructField(_, StructType(fields), _, _)) =>
        fields.exists {
          case StructField(`nestedField`, _, _, _) => true
          case StructField(_, StructType(nestedFields), _, _) =>
            nestedFields.exists(_.name == nestedField)
          case _ => false
        }
      case _ => false
    }
  }

  def getPlayersWithNumbersAndPositions: DataFrame = {

    val selectedDF = gameDF.select($"team.name".as("Team name"), explode($"tactics.lineup").as("lineup"))

    val playerDF = selectedDF.select(
      $"Team name",
      $"lineup.player.id".as("player_id"),
      $"lineup.player.name".as("player_name"),
      $"lineup.position.name".as("player_position"),
      $"lineup.jersey_number".as("player_number")
    ).distinct()

    val groupedDF = playerDF
      .groupBy($"Team name", $"player_name", $"player_number", $"player_id")
      .agg(collect_list($"player_position").as("player_positions"))
      .orderBy($"Team name", $"player_number")

    val formattedDF = groupedDF.withColumn("player_positions", concat_ws(", ", $"player_positions"))

    formattedDF
  }

  def getAllSubstitution: DataFrame = {
    val selectedDF = gameDF.select(
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

    sortedDF
  }

  def getPlayerPassLocalizations: DataFrame = {
    val passEventsDF = gameDF.filter(col("type.name") === "Pass")

    val passEventLocalizationsDF = passEventsDF.select(
      col("player.name").as("player_name"),
      col("player.id").as("player_id"),
      col("location").as("start_location"),
      col("pass.end_location").as("end_location"),
      when(col("pass.outcome").isNull, "Completed").otherwise(col("pass.outcome.name")).as("outcome")
    )

    passEventLocalizationsDF
  }

  def getPlayerPassLocalizations(playerID: Int): DataFrame = {
    getPlayerPassLocalizations.filter(col("player_id") === playerID)
  }

  def getPlayerPassNumberAndAccuracy: DataFrame = {
    val passEventsDf = gameDF.filter(col("type.name") === "Pass")


    val passCountAndAccuracyDf = passEventsDf.groupBy(
        col("team.name").as("team_name"),
        col("player.name").as("player_name")
      )
      .agg(
        count("*").as("total_passes"),
        count(when(col("pass.outcome.name").isNull, 1)).as("accurate_passes")
      ).withColumn("pass_accuracy",
        round(col("accurate_passes") / col("total_passes") * 100))
      .orderBy(col("team_name"), col("pass_accuracy").desc)


    passCountAndAccuracyDf
  }

  def getPlayerPassNumberAndAccuracy(playerID: Int): DataFrame = {
    getPlayerPassNumberAndAccuracy.filter(col("player_id") === playerID)
  }

  def getExactPlayerPassInformation: DataFrame = {
    val passEventsDF = gameDF.filter(col("type.name") === "Pass")

    val exactPassInfo = passEventsDF.groupBy(
        col("team.name").as("team_name"),
        col("player.name").as("player_name"),
        col("player.id").as("player_id")
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
      .withColumn("head_passes_accuracy", coalesce(round(col("accurate_head_passes") / col("total_head_passes") * 100), lit(0)))
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

    exactPassInfo
  }

  def getExactPlayerPassInformation(playerID: Int): DataFrame = {
    getExactPlayerPassInformation.filter(col("player_id") === playerID)
  }

  def getPlayerShotNumberAndAccuracy: DataFrame = {
    val shotsEventsDF = gameDF.filter(col("type.name") === "Shot")


    val shotCountAndAccuracyDf = shotsEventsDF.groupBy(
        col("team.name").as("team_name"),
        col("player.name").as("player_name"),
        col("player.id").as("player_id")
      )
      .agg(
        count("*").as("total_shots"),
        count(when(col("shot.outcome.name") === "Goal", 1)).as("goal"),
        count(when(col("shot.outcome.name").isin("Saved", "Saved off T", "Saved to Post", "Blocked"), 1)).as("blocked_or_saved_by_goalkeeper"),
        count(when(col("shot.outcome.name").isin("Off T", "Post", "Wayward"), 1)).as("not_on_target")
      )
      .withColumn("on_target_accuracy", round((col("total_shots") - col("not_on_target")) / col("total_shots") * 100))
      .withColumn("not_on_target_accuracy", round(col("not_on_target") / col("total_shots") * 100))
      .withColumn("shots_per_goal_ratio", round(col("goal") / col("total_shots") * 100))
      .orderBy(col("team_name"), col("on_target_accuracy").desc)

    shotCountAndAccuracyDf
  }

  def getPlayerShotNumberAndAccuracy(playerID: Int): DataFrame = {
    getPlayerShotNumberAndAccuracy.filter(col("player_id") === playerID)
  }

  def getPlayerShotLocalizations: DataFrame = {
    val shotsEventsDF = gameDF.filter(col("type.name") === "Shot")


    val shotCountAndAccuracyDf = shotsEventsDF.select(
      col("team.name").as("team_name"),
      col("player.id").as("player_id"),
      col("player.name").as("player_name"),
      col("location").as("start_location"),
      col("shot.end_location").as("end_location"),
      col("shot.outcome.name").as("outcome"),
      when(col("shot.outcome.name") === "Goal", 1).otherwise(0).as("is_goal")
    )

    shotCountAndAccuracyDf

  }

  def getPlayerShotLocalizations(playerID: Int): DataFrame = {
    getPlayerShotLocalizations.filter(col("player_id") === playerID)
  }

  def getPlayerTotalTimeWithBall: DataFrame = {
    val carryEventsDF = gameDF.filter(col("type.name") === "Carry")

    val ballCarryTimeDF = carryEventsDF.groupBy(
        col("team.name").as("team_name"),
        col("player.name").as("player_name"),
        col("player.id").as("player_id")
      )
      .agg(
        round(sum(col("duration")) / 60, 2).as("total_time_with_ball")
      )
      .orderBy(col("team_name"), col("total_time_with_ball").desc)

    ballCarryTimeDF
  }

  def getPlayerTotalTimeWithBall(playerID: Int): DataFrame = {
    getPlayerTotalTimeWithBall.filter(col("player_id") === playerID)
  }

  def getPlayerDribbleNumberAndWinRatio: DataFrame = {
    val dribbleEventsDF = gameDF.filter(col("type.name") === "Dribble")

    val dribbleCountAndRatio = dribbleEventsDF.groupBy(
        col("team.name").as("team_name"),
        col("player.name").as("player_name"),
        col("player.id").as("player_id")
      )
      .agg(

        count(when(col("dribble.outcome.name") === "Complete", 1)).as("successful_dribbles"),
        count("*").as("total_dribbles"),

      )
      .withColumn("dribbles_win_ratio", round(col("successful_dribbles") / col("total_dribbles") * 100))
      .orderBy(col("team_name"), col("dribbles_win_ratio").desc)

    dribbleCountAndRatio

  }

  def getPlayerDribbleNumberAndWinRatio(playerID: Int): DataFrame = {
    getPlayerDribbleNumberAndWinRatio.filter(col("player_id") === playerID)
  }

  def getPlayerBallRecoveryNumberAndRatio: DataFrame = {
    val recoveriesEventsDF = gameDF.filter(col("type.name") === "Ball Recovery")

    val recoveriesCountAndRatio = recoveriesEventsDF.groupBy(
        col("team.name").as("team_name"),
        col("player.name").as("player_name")
      )
      .agg(
        count(when(col("ball_recovery.recovery_failure").isNull, 1)).as("successful_ball_recoveries"),
        count("*").as("total_ball_recoveries")
      )
      .withColumn("ball_recovery_ratio", round(col("successful_ball_recoveries") / col("total_ball_recoveries") * 100))
      .orderBy(col("team_name"), col("ball_recovery_ratio").desc)

    recoveriesCountAndRatio
  }

  def getPlayerBallRecoveryNumberAndRatio(playerID: Int): DataFrame = {
    getPlayerBallRecoveryNumberAndRatio.filter(col("player_id") === playerID)
  }

  def getPlayerBlockCountAndRatio: DataFrame = {
    val blocksEventsDF = gameDF.filter(col("type.name") === "Block")
    val blockEventsDFSchema = blocksEventsDF.schema


    val deflectionBlockExists = nestedFieldExists(blockEventsDFSchema, "block", "deflection")
    val offensiveBlockExists = nestedFieldExists(blockEventsDFSchema, "block", "offensive")
    val saveBlockExists = nestedFieldExists(blockEventsDFSchema, "block", "save_block")
    val counterpressBlockExists = nestedFieldExists(blockEventsDFSchema, "block", "counterpress")


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
      col("player.name").as("player_name"),
      col("player.id").as("player_id")
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
      col("player_id"),
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

    finalDF
  }

  def getPlayerBlockCountAndRatio(playerID: Int): DataFrame = {
    getPlayerBlockCountAndRatio.filter(col("player_id") === playerID)
  }

  def getPlayerFoulsCommited: DataFrame = {
    val foulCommitedEventsDF = gameDF.filter(col("type.name") === "Foul Committed")
    val foulCommitedEventsDFSchema = foulCommitedEventsDF.schema

    val sixSecondsFoulExist = nestedFieldExists(foulCommitedEventsDFSchema, "foul_committed", "6 Seconds")
    val backpassPickFoulExist = nestedFieldExists(foulCommitedEventsDFSchema, "foul_committed", "Backpass Pick")
    val dangerousPlayFoulExist = foulCommitedEventsDF.filter(col("foul_committed.type.name") === "Dangerous Play").count() > 0
    val diveFoulExist = foulCommitedEventsDF.filter(col("foul_committed.type.name") === "Dive").count() > 0
    val foulOutExist = foulCommitedEventsDF.filter(col("foul_committed.type.name") === "Foul Out").count() > 0
    val handballFoulExist = foulCommitedEventsDF.filter(col("foul_committed.type.name") === "Handball").count() > 0
    val counterpressFoulExist = nestedFieldExists(foulCommitedEventsDFSchema, "foul_committed", "counterpress")
    val committedOffensiveFoulExist = nestedFieldExists(foulCommitedEventsDFSchema, "foul_committed", "offensive")
    val committedAdvantageFoulExist = nestedFieldExists(foulCommitedEventsDFSchema, "foul_committed", "advantage")
    val committedPenaltyFoulExist = nestedFieldExists(foulCommitedEventsDFSchema, "foul_committed", "penalty")
    val cardFoulExist = nestedFieldExists(foulCommitedEventsDFSchema, "foul_committed", "card")

    var aggCommittedFoulExpression = Seq(
      count("*").as("total_fouls_committed"),
      count(when(col("foul_committed").isNull, 1)).as("standard_fouls")
    )

    if (sixSecondsFoulExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.type.name") === "6 Seconds", 1)).as("6_seconds_fouls")
    }

    if (backpassPickFoulExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.type.name") === "Backpass Pick", 1)).as("backpass_pick_fouls")
    }

    if (dangerousPlayFoulExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.type.name") === "Dangerous Play", 1)).as("dangerous_play_fouls")
    }

    if (diveFoulExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.type.name") === "Dive", 1)).as("dive_fouls")
    }

    if (foulOutExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.type.name") === "Foul Out", 1)).as("fouls_out")
    }

    if (handballFoulExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.type.name") === "Handball", 1)).as("handball_fouls")
    }

    if (counterpressFoulExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.counterpress").isNotNull, 1)).as("counterpress_fouls")
    }

    if (committedOffensiveFoulExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.offensive").isNotNull, 1)).as("offensive_fouls")
    }

    if (committedOffensiveFoulExist) {

      aggCommittedFoulExpression :+= count(when(col("foul_committed.advantage").isNotNull, 1)).as("advantage_fouls")
    }

    if (committedPenaltyFoulExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.penalty").isNotNull, 1)).as("penalty_fouls")
    }

    if (cardFoulExist) {
      aggCommittedFoulExpression :+= count(when(col("foul_committed.card").isNotNull, 1)).as("card_fouls")
    }

    val foulCommittedPerTypeCount = foulCommitedEventsDF.groupBy(
      col("team.name").as("team_name"),
      col("player.name").as("player_name"),
      col("player.id").as("player_id")
    ).agg(aggCommittedFoulExpression.head, aggCommittedFoulExpression.tail: _*)

    var withCommittedFoulsRatios = foulCommittedPerTypeCount
      .withColumn("standard_foul_ratio", round(col("standard_fouls") / col("total_fouls_committed") * 100, 2))

    if (sixSecondsFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("six_seconds_foul_ratio", round(col("six_seconds_fouls") / col("total_fouls_committed") * 100, 2))
    }

    if (backpassPickFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("backpass_pick_foul_ratio", round(col("backpass_pick_fouls") / col("total_fouls_committed") * 100, 2))
    }

    if (dangerousPlayFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("dangerous_play_foul_ratio", round(col("dangerous_play_fouls") / col("total_fouls_committed") * 100, 2))
    }

    if (diveFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("dive_foul_ratio", round(col("dive_fouls") / col("total_fouls_committed") * 100, 2))
    }

    if (foulOutExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("foul_out_ratio", round(col("fouls_out") / col("total_fouls_committed") * 100, 2))
    }

    if (handballFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("handball_foul_ratio", round(col("handball_fouls") / col("total_fouls_committed") * 100, 2))
    }

    if (counterpressFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("counterpress_foul_ratio", round(col("counterpress_fouls") / col("total_fouls_committed") * 100, 2))
    }

    if (committedOffensiveFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("offensive_foul_ratio", round(col("offensive_fouls") / col("total_fouls_committed") * 100, 2))
    }

    if (committedAdvantageFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("advantage_foul_ratio", round(col("advantage_fouls") / col("total_fouls_committed") * 100, 2))
    }

    if (committedPenaltyFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("penalty_foul_ratio", round(col("penalty_fouls") / col("total_fouls_committed") * 100, 2))
    }

    if (cardFoulExist) {

      withCommittedFoulsRatios = withCommittedFoulsRatios.withColumn("card_foul_ratio", round(col("card_fouls") / col("total_fouls_committed") * 100, 2))
    }
    val selectedCommittedFoulsColumns = Seq(
      col("team_name"),
      col("player_name"),
      col("player_id"),
      col("total_fouls_committed"),
      col("standard_fouls"),
      col("standard_foul_ratio")
    ) ++ (
      if (sixSecondsFoulExist) Seq(col("six_seconds_fouls"), col("six_seconds_fouls_ratio")) else Seq()
      ) ++ (

      if (backpassPickFoulExist) Seq(col("backpass_pick_fouls"), col("backpass_pick_foul_ratio")) else Seq()

      ) ++ (

      if (dangerousPlayFoulExist) Seq(col("dangerous_play_fouls"), col("dangerous_play_foul_ratio")) else Seq()

      ) ++ (

      if (diveFoulExist) Seq(col("dive_fouls"), col("dive_fouls_ratio")) else Seq()

      ) ++ (

      if (foulOutExist) Seq(col("fouls_out"), col("fouls_out_ratio")) else Seq()

      ) ++ (

      if (handballFoulExist) Seq(col("handball_fouls"), col("handball_foul_ratio")) else Seq()

      ) ++ (

      if (counterpressFoulExist) Seq(col("counterpress_fouls"), col("counterpress_foul_ratio")) else Seq()

      ) ++ (

      if (committedOffensiveFoulExist) Seq(col("offensive_fouls"), col("offensive_foul_ratio")) else Seq()

      ) ++ (

      if (committedAdvantageFoulExist) Seq(col("advantage_fouls"), col("advantage_foul_ratio")) else Seq()

      ) ++ (

      if (committedPenaltyFoulExist) Seq(col("penalty_fouls"), col("penalty_foul_ratio")) else Seq()

      ) ++ (

      if (cardFoulExist) Seq(col("card_fouls"), col("card_foul_ratio")) else Seq()

      )

    val finalCommittedFoulsDF = withCommittedFoulsRatios.select(selectedCommittedFoulsColumns: _*)
      .orderBy(col("team_name"), col("total_fouls_committed").desc)

    finalCommittedFoulsDF
  }

  def getPlayerFoulsCommited(playerID: Int): DataFrame = {
    getPlayerFoulsCommited.filter(col("player_id") === playerID)
  }

  def getPlayerFoulsWon: DataFrame = {
    val foulWonEventsDF = gameDF.filter(col("type.name") === "Foul Won")

    val foulWonEventsDFSchema = foulWonEventsDF.schema
    val wonDefensiveFoulExist = nestedFieldExists(foulWonEventsDFSchema, "foul_won", "defensive")
    val wonAdvantageFoulExist = nestedFieldExists(foulWonEventsDFSchema, "foul_won", "advantage")
    val wonPenaltyFoulExist = nestedFieldExists(foulWonEventsDFSchema, "foul_won", "penalty")

    var aggWonFoulExpression = Seq(
      count("*").as("total_fouls_won"),
      count(when(col("foul_won").isNull, 1)).as("standard_fouls")
    )


    if (wonAdvantageFoulExist) {

      aggWonFoulExpression :+= count(when(col("foul_won.advantage").isNotNull, 1)).as("advantage_fouls")
    }

    if (wonDefensiveFoulExist) {

      aggWonFoulExpression :+= count(when(col("foul_won.defensive").isNotNull, 1)).as("defensive_fouls")
    }

    if (wonPenaltyFoulExist) {

      aggWonFoulExpression :+= count(when(col("foul_won.penalty").isNotNull, 1)).as("penalty_fouls")
    }

    val foulWonPerTypeCount = foulWonEventsDF.groupBy(
      col("team.name").as("team_name"),
      col("player.name").as("player_name"),
      col("player.id").as("player_id")
    ).agg(aggWonFoulExpression.head, aggWonFoulExpression.tail: _*)

    var withWonFoulsRatio = foulWonPerTypeCount
      .withColumn("standard_foul_ratio", round(col("standard_fouls") / col("total_fouls_won") * 100, 2))


    if (wonDefensiveFoulExist) {

      withWonFoulsRatio = withWonFoulsRatio.withColumn("defensive_foul_ratio", round(col("defensive_fouls") / col("total_fouls_won") * 100, 2))
    }


    if (wonAdvantageFoulExist) {

      withWonFoulsRatio = withWonFoulsRatio.withColumn("advantage_foul_ratio", round(col("advantage_fouls") / col("total_fouls_won") * 100, 2))
    }


    if (wonPenaltyFoulExist) {

      withWonFoulsRatio = withWonFoulsRatio.withColumn("penalty_foul_ratio", round(col("penalty_fouls") / col("total_fouls_won") * 100, 2))
    }

    val selectedWonFoulsColumns = Seq(
      col("team_name"),
      col("player_name"),
      col("player_id"),
      col("total_fouls_won"),
      col("standard_fouls"),
      col("standard_foul_ratio")
    ) ++ (
      if (wonDefensiveFoulExist) Seq(col("defensive_fouls"), col("defensive_foul_ratio")) else Seq()

      ) ++ (

      if (wonAdvantageFoulExist) Seq(col("advantage_fouls"), col("advantage_foul_ratio")) else Seq()
      ) ++ (

      if (wonPenaltyFoulExist) Seq(col("penalty_fouls"), col("penalty_foul_ratio")) else Seq()

      )


    val finalWonFoulsDF = withWonFoulsRatio.select(selectedWonFoulsColumns: _*)
      .orderBy(col("team_name"), col("total_fouls_won").desc)

    finalWonFoulsDF

  }

  def getPlayerFoulsWon(playerID: Int): DataFrame = {
    getPlayerFoulsWon.filter(col("player_id") === playerID)
  }

  def getPlayersPositions: DataFrame = {
    val playerPitchLocationDF = gameDF.select(
      col("team.name").as("team_name"),
      col("player.name").as("player_name"),
      col("player.id").as("player_id"),
      col("location")
    ).where(
      col("team_name").isNotNull &&
        col("player_name").isNotNull &&
        col("location").isNotNull
    )
    playerPitchLocationDF
  }

  def getPlayerPressures: DataFrame = {
    val pressureDF = gameDF.select(
      col("player.id").as("player_id"),
      col("player.name").as("player_name"),
      col("location")
    ).where(
      col("type.name").isNotNull &&
      col("type.name") === "Pressure"
    )

    pressureDF
  }

  def getPlayerPressures(playerID: Int): DataFrame = {
    getPlayerPressures.filter(col("player_id") === playerID)
  }
  def getPlayersPositions(playerID: Int): DataFrame = {
    getPlayersPositions.filter(col("player_id") === playerID)
  }
}
