  package agh.scala.footballanalyzer

  import org.apache.spark.sql.SparkSession
  object Main {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("football-analyzer")
        .master("local[*]")
        .config("spark.driver.bindAdress","127.0.0.1")
        .getOrCreate()
      val json_path = "src/main/data/data.json"
      val gameDF = spark.read.option("multiLine",value = true).json(json_path)
      gameDF.show()

      spark.stop()
    }
  }