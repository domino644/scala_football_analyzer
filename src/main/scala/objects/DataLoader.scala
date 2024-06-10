package agh.scala.footballanalyzer
package objects

import scala.io.Source
import scala.util.{Try, Using}

object DataLoader {
  def fromURLAsString(url: String): String = {
    val json: Try[String] = Using(Source.fromURL(url)) { source => source.mkString }

    json match {
      case scala.util.Success(content) => content
      case scala.util.Failure(exception) => println(s"Error occurred: ${exception.getMessage}")
        throw exception
    }
  }
}
