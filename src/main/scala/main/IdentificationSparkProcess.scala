package main

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.jgrapht.Graph
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}
import org.jgrapht.alg.isomorphism.VF2SubgraphIsomorphismInspector
import scala.jdk.CollectionConverters._
import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

object IdentificationSparkProcess {

    // Helper for JSON reporting
    private case class TimingData(
                                   timestamp: String,
                                   order: Int,
                                   targetLabel: String,
                                   numWorkers: Int,
                                   timings: Map[String, Double],
                                   totalTime: Double,
                                   matchesFound: Long
                                 )

    // Move PatternGraph here to ensure it is serializable for workers
    private case class PatternGraph(graph6: String, jgraph: Graph[String, DefaultEdge]) extends Serializable

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
  "target_label": "${data.targetLabel}",
  "cluster_config": {
    "num_workers": ${data.numWorkers}
  },
  "results": {
    "matches_found": ${data.matchesFound}
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

    def findHighlyIrregularSubgraphs(n: Int,
                                     subgraphsPath: String,
                                     targetGraphPath: String,
                                     outputPath: String,
                                     spark: SparkSession,
                                     timings: mutable.Map[String, Double]
                                    ): Long = {

        // Step 1: Load pattern graphs
        var t0 = System.nanoTime()
        println("\n[1] Loading pattern graphs...")
        val patterns = loadPatternGraphs(subgraphsPath, n, spark)
        val patternsRDD = spark.sparkContext.parallelize(patterns.zipWithIndex.toIndexedSeq)
        var t1 = logTime("load_patterns", t0, timings)

        // Step 2: Load the big graph
        t0 = System.nanoTime()
        println("\n[2] Loading target graph...")
        val graph = loadTargetGraph(targetGraphPath, spark)
        spark.sparkContext.setCheckpointDir("file:///data/checkpoints")
        t1 = logTime("load_target_and_checkpoint", t0, timings)

        // Step 3: Find connected components
        t0 = System.nanoTime()
        println("\n[3] Finding connected components...")
        val componentsRDD = findConnectedComponents(graph, n, spark)
        val componentCount = componentsRDD.count()
        t1 = logTime("connected_components_count", t0, timings)

        // Step 4: Process (components, patterns) in parallel
        t0 = System.nanoTime()
        println(s"\n[4] Processing ($componentCount components x ${patterns.length} patterns) via Cartesian Product...")
        val componentPatternPairs = componentsRDD.cartesian(patternsRDD)

        val matchesRDD = componentPatternPairs.flatMap {
            case ((componentId, vertices, edges), (patternObj, _)) =>
                findMatches(componentId, vertices, edges, patternObj)
        }.cache()

        val totalMatches = matchesRDD.count()
        t1 = logTime("isomorphism_matching_total", t0, timings)

        if (totalMatches > 0) {
            t0 = System.nanoTime()
            println("\n[5] Saving results...")
            saveMatches(matchesRDD, outputPath, spark)
            logTime("save_matches_parquet", t0, timings)
        }

        matchesRDD.unpersist()
        totalMatches
    }

    private def loadPatternGraphs(path: String, n: Int, spark: SparkSession): Array[PatternGraph] = {
        import spark.implicits._
        val edgesDF = spark.read.parquet(s"$path/edges")
        val graph6Ids = edgesDF.select("graph6").distinct().as[String].collect()

        graph6Ids.map { graph6 =>
            val patternEdges = edgesDF
              .filter($"graph6" === graph6)
              .select("src_vertex", "dst_vertex")
              .as[(Int, Int)]
              .collect()

            val jgraph = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge])
            (0 until n).foreach(v => jgraph.addVertex(v.toString))
            patternEdges.foreach { case (src, dst) => jgraph.addEdge(src.toString, dst.toString) }
            PatternGraph(graph6, jgraph)
        }
    }

    private def loadTargetGraph(path: String, spark: SparkSession): GraphFrame = {
        val vertices = spark.read.parquet(s"$path/vertices.parquet")
        val edges = spark.read.parquet(s"$path/edges.parquet")
        GraphFrame(vertices, edges)
    }

    private def findConnectedComponents(graph: GraphFrame, minOrder: Int, spark: SparkSession):
    RDD[(Long, Array[String], Array[(String, String)])] = {
        import spark.implicits._
        val components = graph.connectedComponents.run()
        val componentSizes = components.groupBy("component").agg(count("*").as("size")).filter($"size" >= minOrder).select("component")
        val largeComponents = components.join(componentSizes, "component").select("component", "id")

        val componentEdges = graph.edges
          .join(largeComponents.select($"id".as("src"), $"component".as("src_comp")), "src")
          .join(largeComponents.select($"id".as("dst"), $"component".as("dst_comp")), "dst")
          .filter($"src_comp" === $"dst_comp")
          .select($"src_comp".as("component"), $"src", $"dst")

        val verticesGrouped = largeComponents.groupBy("component").agg(collect_list(col("id").cast("string")).as("vertices"))
        val edgesGrouped = componentEdges.groupBy("component").agg(collect_list(struct(col("src").cast("string"), col("dst").cast("string"))).as("edges"))

        verticesGrouped.join(edgesGrouped, "component").select("component", "vertices", "edges")
          .as[(Long, Seq[String], Seq[(String, String)])].rdd
          .map { case (id, v, e) => (id, v.toArray, e.toArray) }
    }

    private def findMatches(componentId: Long, vertices: Array[String], edges: Array[(String, String)], pattern: PatternGraph): Array[(Long, String, Map[Int, String])] = {
        val targetGraph = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge])
        vertices.foreach(targetGraph.addVertex); edges.foreach { case (u, v) => targetGraph.addEdge(u, v) }
        val inspector = new VF2SubgraphIsomorphismInspector(targetGraph, pattern.jgraph, true)
        inspector.getMappings.asScala.take(1).map { mapping =>
            val resultMap = (0 until pattern.jgraph.vertexSet().size()).map { pV =>
                val pVStr = pV.toString
                pV -> mapping.getVertexCorrespondence(pVStr, false)
            }.toMap
            (componentId, pattern.graph6, resultMap)
        }.toArray
    }

    private def saveMatches(matchesRDD: RDD[(Long, String, Map[Int, String])], outputPath: String, spark: SparkSession): Unit = {
        import spark.implicits._
        matchesRDD.map { case (cId, pId, m) =>
            (cId, pId, m.toSeq.sortBy(_._1).map { case (k, v) => s"$k->$v" }.mkString(";"))
        }.toDF("component_id", "pattern_id", "mapping").coalesce(1).write.mode("overwrite").parquet(outputPath)
    }

    def main(args: Array[String]): Unit = {
        val subgraphOrder = if (args.length > 0) args(0).toInt else 10
        val targetGraphLabel = if (args.length > 1) args(1) else "small"
        val manualNumWorkers = if (args.length > 2) args(2).toInt else 0

        val subgraphsPath = s"/data/classified/order=$subgraphOrder"
        val targetGraphPath = s"/data/target/$targetGraphLabel"
        val outputPath = s"/data/matches/order=${subgraphOrder}_$targetGraphLabel"

        val timings = mutable.Map[String, Double]()
        var sparkSession: SparkSession = null
        var matchesFound: Long = 0

        try {
            val t0_session = System.nanoTime()
            sparkSession = SparkSessionClusterFactory.createSession("Identification")
            timings("spark_session_creation") = (System.nanoTime() - t0_session) / 1e9d

            val t0_process = System.nanoTime()
            matchesFound = findHighlyIrregularSubgraphs(subgraphOrder, subgraphsPath, targetGraphPath, outputPath, sparkSession, timings)
            timings("identification_process_total") = (System.nanoTime() - t0_process) / 1e9d

            val t0_stop = System.nanoTime()
            SparkSessionClusterFactory.stopSession(sparkSession)
            val totalTime = (System.nanoTime() - t0_session) / 1e9d
            timings("spark_session_shutdown") = (System.nanoTime() - t0_stop) / 1e9d
            timings("total_time") = totalTime

            val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))
            val timingData = TimingData(timestamp, subgraphOrder, targetGraphLabel, manualNumWorkers, timings.toMap, totalTime, matchesFound)
            saveTimingToJson(timingData, s"/data/timing_results/identification_order${subgraphOrder}_${targetGraphLabel}_workers${manualNumWorkers}_$timestamp.json")

        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            if (sparkSession != null) sparkSession.stop()
        }
    }
}