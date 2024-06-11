package agh.scala.footballanalyzer
package classes

import org.apache.spark.sql.{DataFrame, SparkSession}


class CompetitionHolder(spark: SparkSession) extends Holder(spark) {
  override protected var baseURL: String = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/competitions.json"
  initDF()

  def getCompetitions: DataFrame =
    DF.select(
      "competition_id",
      "season_id",
      "competition_name",
      "country_name",
      "season_name"
    )
}
