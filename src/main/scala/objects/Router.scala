package agh.scala.footballanalyzer
package objects

import classes._
import objects.utils.{DataFrameParser, SparkSessionSingleton}

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContextExecutor

object Router {
  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-http-server")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext
  val spark: SparkSession = SparkSessionSingleton.createOrGetInstance

  import spark.implicits._

  private val competitionHolder: CompetitionHolder = new CompetitionHolder(spark)
  private val matchHolder: MatchHolder = new MatchHolder(spark)
  private val eventHolder: EventHolder = new EventHolder(spark)
  private val seasonHolder: SeasonHolder = new SeasonHolder(spark)
  private val footballAnalyzer: FootballAnalyzer = new FootballAnalyzer(spark)

  private def getHomeRoute: Route = {
    path("") {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<div><h1>football-analyzer-api</h1><ul><li>competitions</li><li>matches?seasonID;competitionID</li><li>events?eventID;stat</li></ul></div>"))
    }
  }

  private def getCompetitionsRoute: Route = {
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
    }
  }

  private def getMatchesRoute: Route = {
    path("matches") {
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
    }
  }

  private def matchEvents(playerID: Int, stat: String, holder: Holder): DataFrame = {
    var events: DataFrame = spark.emptyDataFrame
    if (playerID == -1) {
      stat match {
        case "all" => events = holder.getDF
        case "players" => events = footballAnalyzer.getPlayersWithNumbersAndPositions
        case "subs" => events = footballAnalyzer.getAllSubstitution
        case "pass_acc" => events = footballAnalyzer.getPlayerPassNumberAndAccuracy
        case "pass_info" => events = footballAnalyzer.getExactPlayerPassInformation
        case "pass_localizations" => events = footballAnalyzer.getPlayerPassLocalizations
        case "shot" => events = footballAnalyzer.getPlayerShotNumberAndAccuracy
        case "shot_localizations" => events = footballAnalyzer.getPlayerShotLocalizations
        case "possesion" => events = footballAnalyzer.getPlayerTotalTimeWithBall
        case "dribble" => events = footballAnalyzer.getPlayerDribbleNumberAndWinRatio
        case "recovery" => events = footballAnalyzer.getPlayerBallRecoveryNumberAndRatio
        case "block" => events = footballAnalyzer.getPlayerBlockCountAndRatio
        case "fouls_commit" => events = footballAnalyzer.getPlayerFoulsCommited
        case "fouls_won" => events = footballAnalyzer.getPlayerFoulsWon
        case "position" => events = footballAnalyzer.getPlayersPositions
        case "pressure" => events = footballAnalyzer.getPlayerPressures
        case _ => events = Seq(s"unknown stat: $stat").toDF("error")
      }
    } else {
      stat match {
        case "all" => events = holder.getDF
        case "players" => events = footballAnalyzer.getPlayersWithNumbersAndPositions
        case "subs" => events = footballAnalyzer.getAllSubstitution
        case "pass_acc" => events = footballAnalyzer.getPlayerPassNumberAndAccuracy(playerID)
        case "pass_info" => events = footballAnalyzer.getExactPlayerPassInformation(playerID)
        case "pass_localizations" => events = footballAnalyzer.getPlayerPassLocalizations(playerID)
        case "shot" => events = footballAnalyzer.getPlayerShotNumberAndAccuracy(playerID)
        case "shot_localizations" => events = footballAnalyzer.getPlayerShotLocalizations(playerID)
        case "possesion" => events = footballAnalyzer.getPlayerTotalTimeWithBall(playerID)
        case "dribble" => events = footballAnalyzer.getPlayerDribbleNumberAndWinRatio(playerID)
        case "recovery" => events = footballAnalyzer.getPlayerBallRecoveryNumberAndRatio(playerID)
        case "block" => events = footballAnalyzer.getPlayerBlockCountAndRatio(playerID)
        case "fouls_commit" => events = footballAnalyzer.getPlayerFoulsCommited(playerID)
        case "fouls_won" => events = footballAnalyzer.getPlayerFoulsWon(playerID)
        case "position" => events = footballAnalyzer.getPlayersPositions(playerID)
        case "pressure" => events = footballAnalyzer.getPlayerPressures(playerID)
        case _ => events = Seq(s"unknown stat: $stat").toDF("error")
      }
    }
    events
  }

  private def getEventsRoute: Route = {
    path("events") {
      parameters("eventID".as[Int], "stat".as[String], "playerID".as[Int].?) {
        (eventID, stat, playerIDOpt) => {
          val playerID = playerIDOpt.getOrElse(-1)
          eventHolder.setEventID(eventID)
          footballAnalyzer.setGameDF(eventHolder.getDF)
          get {
            val stringifiedJSON: String = DataFrameParser.DFtoJsonString(matchEvents(playerID, stat, eventHolder))
            complete(HttpEntity(ContentTypes.`application/json`, stringifiedJSON))
          }
        }
      }
    }
  }

  private def getSeasonRoute: Route = {
    path("season") {
      parameters("competitionID".as[Int], "seasonID".as[Int], "stat".as[String], "playerID".as[Int].?) {
        (competitionID, seasonID, stat, playerIDOpt) => {
          val playerID = playerIDOpt.getOrElse(-1)
          seasonHolder.setParams(competitionID = competitionID, seasonID = seasonID)
          footballAnalyzer.setGameDF(seasonHolder.getDF)
          get {
            val stringifiedJSON: String = DataFrameParser.DFtoJsonString(matchEvents(playerID, stat, seasonHolder))
            complete(HttpEntity(ContentTypes.`application/json`, stringifiedJSON))
          }
        }
      }
    }
  }

  def getRoutes: Route =
    getHomeRoute ~ getCompetitionsRoute ~ getMatchesRoute ~ getEventsRoute ~ getSeasonRoute
}
