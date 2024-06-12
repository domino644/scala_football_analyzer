package agh.scala.footballanalyzer

import classes.SeasonHolder
import objects.utils.SparkSessionSingleton

object Main {
  def main(arg: Array[String]): Unit = {
    val spark = SparkSessionSingleton.createOrGetInstance

    val seasonHolder: SeasonHolder = new SeasonHolder(spark)

    seasonHolder.setParams(seasonID = 281, competitionID = 9)
    //    val matchHolder: MatchHolder = new MatchHolder(spark)
    //    matchHolder.setParams(seasonID = 281, competitionID = 9)
    //    println(matchHolder.getDF.select("match_id").collect().mkString("Array(", ", ", ")"))
  }
}
