package agh.scala.footballanalyzer
package classes

import org.apache.spark.sql.SparkSession


class EventHolder(spark: SparkSession, var eventID: Long = 0) extends Holder(spark) {
  override protected var baseURL: String = s"https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/$eventID.json"
  if (eventID != 0) {
    initDF()
  }

  private def setBaseURL(): Unit =
    baseURL = s"https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/$eventID.json"


  def setEventID(eventID: Long): Unit = {
    this.eventID = eventID
    setBaseURL()
    initDF()
  }

}
