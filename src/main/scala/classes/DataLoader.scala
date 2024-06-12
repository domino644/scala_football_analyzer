package agh.scala.footballanalyzer
package classes

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer



class DataLoader(spark:SparkSession, competitionID:Int, seasonID:Int) {



    private def getMatchesID: Array[Long]  = {

        val matchHolder = new MatchHolder(spark, competitionID = competitionID, seasonID = seasonID)
        val allMatchID = matchHolder.getDF.select("match_id").collect()
        val matchIDs = allMatchID.map(row => row.getLong(0))

        println(matchIDs.mkString("Array(", ", ", ")"))
        matchIDs

    }


    def getAllEventsDF: DataFrame  = {
        val matchesID = getMatchesID
        val events = ListBuffer[DataFrame]()


        for (matchID <- matchesID) {

            val eventHolder: EventHolder = new EventHolder(spark, matchID)
            val eventDF = eventHolder.getDF
            events += eventDF

        }

        val finalDF = events.reduce((df1, df2) => df1.unionByName(df2, allowMissingColumns = true))

        finalDF

    }


}
