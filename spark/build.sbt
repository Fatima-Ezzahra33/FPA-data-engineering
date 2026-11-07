
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "fpa-spark",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "4.2.0" % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.2.0",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),

    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
