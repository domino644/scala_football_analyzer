package agh.scala.footballanalyzer
package objects

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn


object Server {

  def main(arg: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-http-server")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext
    val routes = Router.getRoutes
    val bindingFuture = Http().newServerAt("localhost", 8080).bind(routes)
    println(s"Server now online. Please navigate to http://localhost:8080\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

