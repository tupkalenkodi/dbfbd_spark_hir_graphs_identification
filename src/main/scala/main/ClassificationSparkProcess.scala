package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable


object ClassificationSparkProcess {

    import Utilities.isHighlyIrregular

    // Case class to store timing information
    private case class TimingData(
                           timestamp: String,
                           order: Int,
                           numWorkers: Int,
                           timings: Map[String, Double],
                           totalTime: Double,
                           graphsFound: Long,
                         )

    // UDF for highly irregular classification
    private val isHighlyIrregularUDF = udf((edgesStr: String, order: Int) => {
        isHighlyIrregular(edgesStr, order)
    })

    // UDF to parse edges string into array of structs
    private val parseEdgesUDF = udf((edgesStr: String) => {
        if (edgesStr == null || edgesStr.isEmpty) {
            Array.empty[(Int, Int)]
        } else {
            edgesStr.split(";").map { edge =>
                val parts = edge.split(",")
                (parts(0).toInt, parts(1).toInt)
            }
        }
    })

    // Helper function to parse edges locally
    private def parseEdgesLocal(edgesStr: String): Seq[(Int, Int)] = {
        if (edgesStr == null || edgesStr.isEmpty) {
            Seq.empty
        } else {
            edgesStr.split(";").map { edge =>
                val parts = edge.split(",")
                (parts(0).toInt, parts(1).toInt)
            }
        }
    }

    // Utility function to log elapsed time
    private def logTime(label: String, startTime: Long, timings: mutable.Map[String, Double]): Long = {
        val endTime = System.nanoTime()
        val elapsed = (endTime - startTime) / 1e9d
        println(f"   $label: $elapsed%.2f seconds")
        timings(label) = elapsed
        endTime
    }

    // Function to get number of workers from Spark context
    private def getNumWorkers(spark: SparkSession): Int = {
        val sc = spark.sparkContext
        // Get the number of all executor infos, including the driver
        val executorInfos = sc.statusTracker.getExecutorInfos
        // Subtract 1 to exclude the driver from the count
        val numberOfExecutors = executorInfos.length - 1
        numberOfExecutors
    }

    // Function to save timing data as JSON
    private def saveTimingToJson(data: TimingData, outputFile: String): Unit = {
        val json = s"""{
  "timestamp": "${data.timestamp}",
  "order": ${data.order},
  "cluster_config": {
    "num_workers": ${data.numWorkers},
  },
  "results": {
    "graphs_found": ${data.graphsFound},
  },
  "timings": {
${data.timings.map { case (k, v) => s"""    "$k": $v""" }.mkString(",\n")}
  },
  "total_time_seconds": ${data.totalTime}
}"""

        val writer = new PrintWriter(new File(outputFile))
        try {
            writer.write(json)
            println(s"\n  Timing data saved to: $outputFile")
        } finally {
            writer.close()
        }
    }

    def classifyGraphs(order: Int, inputDir: String, outputDir: String,
                       spark: SparkSession, timings: mutable.Map[String, Double]): Long = {
        import spark.implicits._

        println("=" * 70)
        println(s"CLASSIFYING HIGHLY IRREGULAR GRAPHS")
        println("=" * 70)

        val schema = StructType(Array(
            StructField("graph6", StringType, nullable = false),
            StructField("graph_order", IntegerType, nullable = false),
            StructField("edges", StringType, nullable = false)
        ))

        println("\n" + "*" * 70)

        // TIMING: Read data
        var t0 = System.nanoTime()
        println(s"Reading search space from $inputDir...")
        val df = spark.read
          .schema(schema)
          .parquet(inputDir)
        var t1 = logTime("read_parquet", t0, timings)

        // TIMING: Classification
        t0 = System.nanoTime()
        println("Classifying graphs...")
        val resultDF = df.withColumn(
            "is_highly_irregular",
            isHighlyIrregularUDF(col("edges"), col("graph_order"))
        )

        val highlyIrregularDF = resultDF
          .filter(col("is_highly_irregular") === true)
          .select("graph6", "graph_order", "edges")

        val count = highlyIrregularDF.count()
        t1 = logTime("classification_and_count", t0, timings)

        println(f"\nFound $count%,d highly irregular graphs of order $order")

        if (count == 0) {
            println("No highly irregular graphs found.")
            println("=" * 70)
            return count
        }

        // TIMING: Collect to driver
        t0 = System.nanoTime()
        println("Collecting data to driver...")
        val graphData = highlyIrregularDF
          .select("graph6", "edges")
          .as[(String, String)]
          .collect()
        t1 = logTime("collect_to_driver", t0, timings)

        // TIMING: Prepare vertices data
        t0 = System.nanoTime()
        println("Preparing vertices data...")
        val verticesData = graphData.flatMap { case (graph6, _) =>
            (0 until order).map { vertexId =>
                (vertexId, graph6)
            }
        }
        t1 = logTime("prepare_vertices", t0, timings)

        // TIMING: Prepare edges data
        t0 = System.nanoTime()
        println("Preparing edges data...")
        val edgesData = graphData.flatMap { case (graph6, edgesStr) =>
            parseEdgesLocal(edgesStr).map { case (src, dst) =>
                (graph6, src, dst)
            }
        }
        t1 = logTime("prepare_edges", t0, timings)

        // TIMING: Write vertices
        t0 = System.nanoTime()
        println(s"Writing vertices (${verticesData.length} rows) to $outputDir/vertices...")
        val verticesDF = verticesData.toSeq.toDF("id", "graph6")
        verticesDF
          .coalesce(1)
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(s"$outputDir/vertices")
        t1 = logTime("write_vertices", t0, timings)

        // TIMING: Write edges
        t0 = System.nanoTime()
        println(s"Writing edges (${edgesData.length} rows) to $outputDir/edges...")
        val edgesDF = edgesData.toSeq.toDF("graph6", "src_vertex", "dst_vertex")
        edgesDF
          .coalesce(1)
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(s"$outputDir/edges")
        t1 = logTime("write_edges", t0, timings)

        println("*" * 70)
        println("\n")
        println("=" * 70)
        println("CLASSIFICATION COMPLETE!")

        count
    }


    def main(args: Array[String]): Unit = {
        // Parse command line argument for order
        val order = if (args.length > 0) args(0).toInt else 10

        val inputDir = s"/data/generated/order=$order"
        val outputDir = s"/data/classified/order=$order"

        println("\n" + "=" * 70)
        println(s"CLASSIFICATION JOB: n=$order")
        println("=" * 70)
        println(s"Input:  $inputDir")
        println(s"Output: $outputDir")
        println("=" * 70)
        println("\nTIMING BREAKDOWN")
        println("=" * 70)

        val timings = mutable.Map[String, Double]()
        var sparkSession: SparkSession = null
        var graphsFound: Long = 0
        var numWorkers = 0

        try {
            // TIMING: Spark session creation
            val t0_session = System.nanoTime()
            println("Creating Spark session...")
            sparkSession = SparkSessionClusterFactory.createSession("Classification")
            val t1_session = System.nanoTime()
            val sessionTime = (t1_session - t0_session) / 1e9d
            timings("spark_session_creation") = sessionTime
            println(f"   Spark session creation: $sessionTime%.2f seconds")
            println()

            // Get cluster configuration
            numWorkers = getNumWorkers(sparkSession)

            println(s" Cluster Configuration:")
            println(s"   Workers: $numWorkers")

            // TIMING: Classification process
            val t0_process = System.nanoTime()
            graphsFound = classifyGraphs(order, inputDir, outputDir, sparkSession, timings)
            val t1_process = System.nanoTime()
            val processTime = (t1_process - t0_process) / 1e9d
            timings("classification_process_total") = processTime

            // TIMING: Spark session shutdown
            val t0_stop = System.nanoTime()
            println("Stopping Spark session...")
            SparkSessionClusterFactory.stopSession(sparkSession)
            sparkSession = null
            val t1_stop = System.nanoTime()
            val stopTime = (t1_stop - t0_stop) / 1e9d
            timings("spark_session_shutdown") = stopTime
            println(f"   Spark session shutdown: $stopTime%.2f seconds")

            // SUMMARY
            val totalTime = (t1_stop - t0_session) / 1e9d
            timings("total_time") = totalTime

            println()
            println("=" * 70)
            println("TIMING SUMMARY")
            println("=" * 70)
            println(f"  Spark Session Creation:  ${timings("spark_session_creation")}%8.2f seconds  (${timings("spark_session_creation")/totalTime*100}%5.1f%%)")
            println(f"  Classification Process:  $processTime%8.2f seconds  (${processTime/totalTime*100}%5.1f%%)")
            println(f"  Spark Session Shutdown:  $stopTime%8.2f seconds  (${stopTime/totalTime*100}%5.1f%%)")
            println(f"  ${"─" * 68}")
            println(f"  TOTAL TIME:              $totalTime%8.2f seconds")
            println("=" * 70)

            // Save timing data to JSON
            val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))
            val resultSetSize = if (graphsFound < 10000) "small" else "large"

            val timingData = TimingData(
                timestamp = timestamp,
                order = order,
                numWorkers = numWorkers,
                timings = timings.toMap,
                totalTime = totalTime,
                graphsFound = graphsFound,
            )

            val outputPath = "/timing_results"
            val jsonFile = s"$outputPath/classification_order${order}_$timestamp.json"
            new File(outputPath).mkdirs() // Create directory if it doesn't exist
            saveTimingToJson(timingData, jsonFile)

        } catch {
            case e: Exception =>
                println(s"\n ERROR: ${e.getMessage}")
                e.printStackTrace()
        } finally {
            if (sparkSession != null) {
                SparkSessionClusterFactory.stopSession(sparkSession)
            }
        }
    }
}