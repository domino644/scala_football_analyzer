package agh.scala.footballanalyzer
package objects

import classes.{CompetitionHolder, EventHolder, FootballAnalyzer, MatchHolder}
import objects.utils.{DataFrameParser, SparkSessionSingleton}

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object Server {

  def main(arg: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-http-server")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext
    val spark: SparkSession = SparkSessionSingleton.createOrGetInstance
    import spark.implicits._
    val competitionHolder: CompetitionHolder = new CompetitionHolder(spark)
    val matchHolder: MatchHolder = new MatchHolder(spark)
    val eventHolder: EventHolder = new EventHolder(spark)
    val footballAnalyzer: FootballAnalyzer = new FootballAnalyzer(spark)
    val route =
      path("competitions") {
        parameter("all".as[Boolean].?) {
          allOpt => {
            val all = allOpt.getOrElse(false)
            get {
              val competitions: DataFrame = if (!all) competitionHolder.getCompetitions else competitionHolder.getDF
              val stringifiedJSON: String = DataFrameParser.DFtoJsonString(competitions)
              complete(HttpEntity(ContentTypes.`application/json`, stringifiedJSON))
            }
          }
        }
      } ~ path("matches") {
        parameters("competitionID".as[Int], "seasonID".as[Int], "all".as[Boolean].?) {
          (competitionID, seasonID, allOpt) => {
            val all: Boolean = allOpt.getOrElse(false)
            get {
              matchHolder.setParams(competitionID = competitionID, seasonID = seasonID)
              val matches = if (!all) matchHolder.getMatchesInfo else matchHolder.getDF
              val stringifiedJSON: String = DataFrameParser.DFtoJsonString(matches)
              complete(HttpEntity(ContentTypes.`application/json`, stringifiedJSON))
            }
          }
        }
      } ~ path("events") {
        parameter("eventID".as[Int], "stat".as[String]) {
          (eventID, stat) => {
            eventHolder.setEventID(eventID)
            footballAnalyzer.setGameDF(eventHolder.getDF)
            var events: DataFrame = spark.emptyDataFrame
            get{
              stat match {
                case "all" => events = eventHolder.getDF
                case "players" => events = footballAnalyzer.getPlayersWithNumbersAndPositions
                case "subs" => events = footballAnalyzer.getAllSubstitution
                case "pass_acc" => events = footballAnalyzer.getPlayerPassNumberAndAccuracy
                case "pass_info" => events = footballAnalyzer.getExactPlayerPassInformation
                case "shot" => events = footballAnalyzer.getPlayerShotNumberAndAccuracy
                case "possesion" => events = footballAnalyzer.getPlayerTotalTimeWithBall
                case "dribble" => events = footballAnalyzer.getPlayerDribbleNumberAndWinRatio
                case "recovery" => events = footballAnalyzer.getPlayerBallRecoveryNumberAndRatio
                case "block" => events = footballAnalyzer.getPlayerBlockCountAndRatio
                case "fouls_commit" => events = footballAnalyzer.getPlayerFoulsCommited
                case "fouls_won" => events = footballAnalyzer.getPlayerFoulsWon
                case "position" => events = footballAnalyzer.getPlayersPositionsCount
                case _ => events = Seq("error", s"unknown stat: $stat").toDF()
              }
              val stringifiedJSON: String = DataFrameParser.DFtoJsonString(events)
              complete(HttpEntity(ContentTypes.`application/json`, stringifiedJSON))
            }
          }
        }
      }

    val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)
    println(s"Server now online. Please navigate to http://localhost:8080\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
