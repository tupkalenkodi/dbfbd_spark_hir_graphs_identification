ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "3.5.3"
val parquet4sVersion = "2.23.0"

lazy val root = (project in file("."))
  .settings(
    name := "graph-classifier-scala",

    libraryDependencies ++= Seq(
      // Spark dependencies
      "org.apache.spark" %% "spark-core" % sparkVersion, // spark
      "org.apache.spark" %% "spark-sql" % sparkVersion, // spark SQL
      "org.apache.spark" %% "spark-graphx" % sparkVersion, // spark GraphX
      "graphframes" % "graphframes" % "0.8.3-spark3.5-s_2.13",  // spark GraphFrames
      "org.jgrapht" % "jgrapht-core" % "1.5.2", // for subgraphs identification

      // Parquet4s - pure Scala Parquet library
      "com.github.mjakubowski84" %% "parquet4s-core" % parquet4sVersion,
      // Scala parallel collections
      "org.scala-lang.modules" %% "scala-parallel-collections" % "1.2.0"
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

    // Java options for Java 17 compatibility with Spark 3.5.3
    javaOptions ++= Seq(
      "-Xms512M",
      "-Xmx4G",
      "-XX:+UseG1GC",
      "-XX:+ExitOnOutOfMemoryError",
      "-XX:+CrashOnOutOfMemoryError",
      "-Dlog4j2.configurationFile=log4j2.properties",
      // Java 17 module exports for Spark compatibility
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    ),

    // Set JAVA_HOME explicitly if needed
    envVars := Map(
      "JAVA_HOME" -> System.getProperty("java.home"),
      "SPARK_LOCAL_IP" -> "127.0.0.1"
    )
  )