package agh.scala.footballanalyzer
package objects

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Path.~
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object Server {
  def main(arg: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-http-server")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext
    val route =
      path("hello") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
        }
      } ~
        path(""){
          get{
            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Home</h1>"))
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
