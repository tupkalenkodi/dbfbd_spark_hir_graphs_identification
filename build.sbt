ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "4.0.1"
val parquet4sVersion = "2.17.0"

lazy val root = (project in file("."))
  .settings(
    name := "graph-classifier-scala",

    libraryDependencies ++= Seq(
      // Spark dependencies
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,

      // GraphFrames for graph processing
      "io.graphframes" % "graphframes" % "0.10.0-spark4-s_2.13",

      // Parquet4s - pure Scala Parquet library
      "com.github.mjakubowski84" %% "parquet4s-core" % parquet4sVersion,

      // Scala parallel collections
      "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
    ),

    // Add resolver for GraphFrames
    resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/",

    // Compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-unused",
      "-encoding", "utf8"
    ),

    // Fork the JVM for running
    fork := true,
    javaOptions ++= Seq(
      "-Xms512M",
      "-Xmx4G",
      "-XX:+UseG1GC",
      "-Dlog4j2.configurationFile=log4j2.properties"
    )
  )