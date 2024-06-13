package agh.scala.footballanalyzer
package objects.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameInitializer {
  def initDFFromURL(spark: SparkSession, url: String): DataFrame = {
    import spark.implicits._
    spark.read.json(Seq(DataLoader.fromURLAsString(url)).toDS())
  }
}
