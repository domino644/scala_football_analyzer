package agh.scala.footballanalyzer
package classes

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.Try


class SeasonHolder(spark: SparkSession, var competitionID: Int = 0, var seasonID: Int = 0) extends Holder(spark) {
  private val matchHolder: MatchHolder = new MatchHolder(spark)
  private val eventHolder: EventHolder = new EventHolder(spark)
  if (competitionID != 0 && seasonID != 0) {
    initDF()
  }

  override protected var baseURL: String = "" //no need for it, but has to be overriden

  private def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  private def getMatchesID: Array[Long] = {
    val allMatchID = matchHolder.getDF.select("match_id").collect()
    val matchIDs = allMatchID.map(row => row.getLong(0))

    matchIDs
  }

  def setParams(seasonID: Int, competitionID: Int): Unit = {
    this.competitionID = competitionID
    this.seasonID = seasonID
    matchHolder.setParams(competitionID = competitionID, seasonID = seasonID)
    initDF()
  }

  override def initDF(): Unit = {
    val matchesID = getMatchesID
    val events = ListBuffer[DataFrame]()


    for (matchID <- matchesID) {

      eventHolder.setEventID(matchID)
      val eventDF = eventHolder.getDF
      events += eventDF

    }

    DF = events.reduce((df1, df2) => df1.unionByName(df2, allowMissingColumns = true))
  }


}
