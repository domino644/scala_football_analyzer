ThisBuild / version := "0.1.0-SNAPSHOT"
val AkkaVersion = "2.9.3"
val AkkaHttpVersion = "10.6.3"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
    .settings(
        name := "football-analyzer",
        idePackagePrefix := Some("agh.scala.footballanalyzer")
    )
resolvers += "Akka library repository".at("https://repo.akka.io/maven")
val sparkVersion = "3.5.1"
libraryDependencies ++= Seq(
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
    "org.apache.spark" %% "spark-core" % sparkVersion,
    // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
    "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion
)