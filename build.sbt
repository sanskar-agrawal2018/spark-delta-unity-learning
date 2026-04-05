



ThisBuild / version := "0.1.0-SNAPSHOT-1"


ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "spark-3.5-spark-3.5.2",

  )



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.2" ,
  "org.apache.spark" %% "spark-sql"  % "3.5.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
  "io.delta" %% "delta-spark" % "3.3.2",
  // Typesafe Config for externalized configuration (secrets, tokens)
  "com.typesafe" % "config" % "1.4.2",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)