package agh.scala.footballanalyzer
package classes

import objects.utils.DataLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Holder(spark: SparkSession){
  protected var DF: DataFrame = spark.emptyDataFrame
  protected var baseURL: String
  def initDF(): Unit = {
    import spark.implicits._
    DF = spark.read.json(Seq(DataLoader.fromURLAsString(baseURL)).toDS())
  }

  def getDF: DataFrame = DF
}
