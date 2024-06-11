package agh.scala.footballanalyzer
package objects.utils

import scala.io.Source
import scala.util.{Try, Using}

object DataLoader {
  def fromURLAsString(url: String): String = {
    val json: Try[String] = Using(Source.fromURL(url)) { source => source.mkString }

    json match {
      case scala.util.Success(content: String) => content
      case scala.util.Failure(_) => s"{'error':'error getting $url'}"
    }
  }
}
