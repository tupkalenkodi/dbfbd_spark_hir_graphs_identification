package main

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

object ClassificationSparkProcess {

    import Utilities.isHighlyIrregular

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

    // UDF to parse edges string into array of structs (Int, Int)
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

    private def logTime(label: String, startTime: Long, timings: mutable.Map[String, Double]): Long = {
        val endTime = System.nanoTime()
        val elapsed = (endTime - startTime) / 1e9d
        println(f"   $label: $elapsed%.2f seconds")
        timings(label) = elapsed
        endTime
    }

    private def saveTimingToJson(data: TimingData, outputFile: String): Unit = {
        val json = s"""{
  "timestamp": "${data.timestamp}",
  "order": ${data.order},
  "cluster_config": {
    "num_workers": ${data.numWorkers}
  },
  "results": {
    "graphs_found": ${data.graphsFound}
  },
  "timings": {
${data.timings.map { case (k, v) => s"""    "$k": $v""" }.mkString(",\n")}
  },
  "total_time_seconds": ${data.totalTime}
}"""
        val file = new File(outputFile)
        file.getParentFile.mkdirs()
        val writer = new PrintWriter(file)
        try { writer.write(json) } finally { writer.close() }
    }

    def classifyGraphs(order: Int, inputDir: String, outputDir: String,
                       spark: SparkSession, timings: mutable.Map[String, Double]): Long = {
        import spark.implicits._

        val schema = StructType(Array(
            StructField("graph6", StringType, nullable = false),
            StructField("graph_order", IntegerType, nullable = false),
            StructField("edges", StringType, nullable = false)
        ))

        // 1. READ
        var t0 = System.nanoTime()
        val df = spark.read.schema(schema).parquet(inputDir)
        var t1 = logTime("read_parquet", t0, timings)

        // 2. CLASSIFY (Distributed)
        t0 = System.nanoTime()
        val highlyIrregularDF = df
          .withColumn("is_highly_irregular", isHighlyIrregularUDF(col("edges"), col("graph_order")))
          .filter(col("is_highly_irregular") === true)
          .select("graph6", "graph_order", "edges")
          .cache() // Cache because we use it for count AND transformations

        val count = highlyIrregularDF.count()
        t1 = logTime("classification_and_count", t0, timings)

        if (count == 0) return 0

        // 3. TRANSFORM VERTICES (Distributed)
        // Replaces the local flatMap. Uses sequence(0, n-1) and explodes it into rows.
        t0 = System.nanoTime()
        val verticesDF = highlyIrregularDF.select(
            explode(sequence(lit(0), col("graph_order") - 1)).as("id"),
            col("graph6")
        )
        t1 = logTime("distributed_prepare_vertices", t0, timings)

        // 4. TRANSFORM EDGES (Distributed)
        // Replaces local parseEdgesLocal.
        t0 = System.nanoTime()
        val edgesDF = highlyIrregularDF.select(
            col("graph6"),
            explode(parseEdgesUDF(col("edges"))).as("edge")
        ).select(
            col("graph6"),
            col("edge._1").as("src_vertex"),
            col("edge._2").as("dst_vertex")
        )
        t1 = logTime("distributed_prepare_edges", t0, timings)

        // 5. WRITE (Distributed)
        t0 = System.nanoTime()
        verticesDF.coalesce(1).write.mode("overwrite").parquet(s"$outputDir/vertices")
        t1 = logTime("write_vertices", t0, timings)

        t0 = System.nanoTime()
        edgesDF.coalesce(1).write.mode("overwrite").parquet(s"$outputDir/edges")
        t1 = logTime("write_edges", t0, timings)

        highlyIrregularDF.unpersist()
        count
    }

    def main(args: Array[String]): Unit = {
        val order = if (args.length > 0) args(0).toInt else 10
        // Accept num_workers as 2nd param, default to 0 if not provided
        val manualNumWorkers = if (args.length > 1) args(1).toInt else 0

        val inputDir = s"/data/generated/order=$order"
        val outputDir = s"/data/classified/order=$order"

        val timings = mutable.Map[String, Double]()
        var sparkSession: SparkSession = null
        var graphsFound: Long = 0

        try {
            val t0_session = System.nanoTime()
            sparkSession = SparkSessionClusterFactory.createSession("Classification")
            val t1_session = System.nanoTime()
            timings("spark_session_creation") = (t1_session - t0_session) / 1e9d

            // PROCESS
            val t0_process = System.nanoTime()
            graphsFound = classifyGraphs(order, inputDir, outputDir, sparkSession, timings)
            val t1_process = System.nanoTime()
            timings("classification_process_total") = (t1_process - t0_process) / 1e9d

            // SHUTDOWN
            val t0_stop = System.nanoTime()
            SparkSessionClusterFactory.stopSession(sparkSession)
            val t1_stop = System.nanoTime()
            val stopTime = (t1_stop - t0_stop) / 1e9d
            timings("spark_session_shutdown") = stopTime

            val totalTime = (t1_stop - t0_session) / 1e9d
            timings("total_time") = totalTime

            // SAVE RESULTS
            val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))
            val timingData = TimingData(timestamp, order, manualNumWorkers, timings.toMap, totalTime, graphsFound)

            saveTimingToJson(timingData, s"/data/timing_results/classification_order${order}_workers${manualNumWorkers}_$timestamp.json")

        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            if (sparkSession != null) sparkSession.stop()
        }
    }
}