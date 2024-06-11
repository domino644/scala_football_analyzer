package agh.scala.footballanalyzer
package classes

import org.apache.spark.sql.{DataFrame, SparkSession}

class MatchHolder(spark: SparkSession, var competitionID: Int = 0, var seasonID: Int = 0) extends Holder(spark) {

  import spark.implicits._

  protected var baseURL = s"https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/$competitionID/$seasonID.json"
  if (competitionID != 0 && seasonID != 0) {
    initDF()
  }

  private def setBaseURL(): Unit = {
    baseURL = s"https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/$competitionID/$seasonID.json"
  }

  def setParams(competitionID: Int, seasonID: Int): Unit = {
    this.competitionID = competitionID
    this.seasonID = seasonID
    setBaseURL()
    initDF()
  }

  def getMatchesInfo: DataFrame =
    DF.select(
      $"match_id",
      $"home_team.home_team_id".as("home_team_id"),
      $"home_team.home_team_name".as("home_team_name"),
      $"away_team.away_team_id".as("away_team_id"),
      $"away_team.away_team_name".as("away_team_name"),
      $"home_score",
      $"away_score"
    )
}
