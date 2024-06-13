package agh.scala.footballanalyzer
package objects.utils

import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

object DataLoader {
  def fromURLAsString(url: String): String = {
    val json: Try[String] = Using(Source.fromURL(url)) { source => source.mkString }

    json match {
      case Success(content: String) => content
      case Failure(_) => s"{'error':'error getting $url'}"
    }
  }
}
