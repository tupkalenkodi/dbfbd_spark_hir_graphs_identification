ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "4.0.1"

lazy val root = (project in file("."))
  .settings(
    name := "graph-classifier-scala",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    ),

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
