package agh.scala.footballanalyzer
package classes

import objects.utils.{DataFrameInitializer, DataLoader}

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Holder(spark: SparkSession){
  protected var DF: DataFrame = spark.emptyDataFrame
  protected var baseURL: String
  def initDF(): Unit = {
    DF = DataFrameInitializer.initDFfromURL(spark, baseURL)
  }

  def getDF: DataFrame = DF
}
