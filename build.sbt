ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "football-analyzer",
    idePackagePrefix := Some("agh.scala.footballanalyzer")
  )

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

