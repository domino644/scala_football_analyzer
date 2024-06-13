package agh.scala.footballanalyzer
package objects.utils

import org.apache.spark.sql.SparkSession


object SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def createOrGetInstance: SparkSession = {
    println("Instance:" + instance)
    if (instance == null) {
      instance = SparkSession.builder()
        .appName("football_analyzer")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    }
    instance
  }
}