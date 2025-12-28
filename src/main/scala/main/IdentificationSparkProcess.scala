package main

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.jgrapht.Graph
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}
import org.jgrapht.alg.isomorphism.VF2SubgraphIsomorphismInspector


object IdentificationSparkProcess {

    def findHighlyIrregularSubgraphs(n: Int,
                                     subgraphsPath: String,
                                     targetGraphPath: String,
                                     outputPath: String,
                                     spark: SparkSession
                                    ): Unit = {
        // Step 1: Load pattern graphs
        println("\n[1] Loading and Broadcasting pattern graphs...")
        val patterns = loadPatternGraphs(subgraphsPath, n, spark)
        val patternsBroadcast = spark.sparkContext.broadcast(patterns)
        println(s"    Loaded and Broadcasted ${patterns.length} pattern graphs")

        // Step 2: Load the big graph
        println("\n[2] Loading target graph...")
        val graph = loadTargetGraph(targetGraphPath, spark)
        println(s"    Loaded target graph")

        // Step 3: Find connected components
        println("\n[3] Finding connected components...")
        val componentsRDD = findConnectedComponents(graph, n, spark)
        val componentCount = componentsRDD.count()
        println(s"    Found $componentCount valid components")

        // Step 4: Process components in parallel
        println(s"\n[4] Processing components in parallel...")
        val matchesRDD = componentsRDD.flatMap { case (componentId, vertices, edges) =>
            findMatchesInComponent(componentId, vertices, edges, patternsBroadcast.value)
        }

        val totalMatches = matchesRDD.count()
        if (totalMatches > 0) {
            // Step 5: Save results
            println("\n[5] Saving results...")
            saveMatches(matchesRDD, outputPath, spark)
        }

        println("\n" + "=" * 70)
        println(s"TOTAL MATCHES FOUND: $totalMatches")
    }

    private case class PatternGraph(graph6: String, jgraph: Graph[String, DefaultEdge])

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
            patternEdges.foreach { case (src, dst) =>
                jgraph.addEdge(src.toString, dst.toString)
            }

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

        val componentSizes = components
          .groupBy("component")
          .agg(count("*").as("size"))
          .filter($"size" >= minOrder)
          .select("component")

        val largeComponents = components
          .join(componentSizes, "component")
          .select("component", "id")
          .cache()

        val componentEdges = graph.edges
          .join(largeComponents.select($"id".as("src"), $"component".as("src_comp")), "src")
          .join(largeComponents.select($"id".as("dst"), $"component".as("dst_comp")), "dst")
          .filter($"src_comp" === $"dst_comp")
          .select($"src_comp".as("component"), $"src", $"dst")
          .cache()

        val verticesGrouped = largeComponents
          .groupBy("component")
          .agg(collect_list("id").as("vertices"))

        val edgesGrouped = componentEdges
          .groupBy("component")
          .agg(collect_list(struct($"src", $"dst")).as("edges"))

        val componentsWithData = verticesGrouped
          .join(edgesGrouped, "component")
          .select("component", "vertices", "edges")

        componentsWithData
          .as[(Long, Seq[String], Seq[(String, String)])]
          .rdd
          .map { case (componentId, verticesSeq, edgesSeq) =>
              val vertices = verticesSeq.toArray
              val edges = edgesSeq.toArray
              (componentId, vertices, edges)
          }
    }

    private def findMatchesInComponent(componentId: Long,
                                       vertices: Array[String],
                                       edges: Array[(String, String)],
                                       patterns: Array[PatternGraph]
                                      ): Array[(Long, String, Map[Int, String])] = {

        val targetGraph = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge])

        vertices.foreach(targetGraph.addVertex)
        edges.foreach { case (u, v) => targetGraph.addEdge(u, v) }

        patterns.flatMap { pattern =>
            val inspector = new VF2SubgraphIsomorphismInspector(
                targetGraph,
                pattern.jgraph,
                true  // induced subgraph
            )

            import scala.jdk.CollectionConverters._
            inspector.getMappings.asScala.take(1000).map { mapping =>
                val resultMap = (0 until pattern.jgraph.vertexSet().size()).map { patternVertex =>
                    val patternVertexStr = patternVertex.toString
                    val targetVertex = mapping.getVertexCorrespondence(patternVertexStr, false)
                    patternVertex -> targetVertex
                }.toMap

                (componentId, pattern.graph6, resultMap)
            }.toArray
        }
    }


    private def saveMatches(matchesRDD: RDD[(Long, String, Map[Int, String])],
                            outputPath: String,
                            spark: SparkSession): Unit = {
        import spark.implicits._

        val matchesDF = matchesRDD.map { case (componentId, patternId, mapping) =>
            val mappingStr = mapping.toSeq.sortBy(_._1)
              .map { case (k, v) => s"$k->$v" }
              .mkString(";")
            (componentId, patternId, mappingStr)
        }.toDF("component_id", "pattern_id", "mapping")

        matchesDF
          .coalesce(1)
          .write
          .mode("overwrite")
          .option("compression", "none")
          .parquet(outputPath)
    }

    def main(args: Array[String]): Unit = {
        val subgraphOrder = 10
        val subgraphsPath = s"data/classified/order=$subgraphOrder"
        val targetGraphPath = "data/target"
        val outputPath = s"data/matches/order=$subgraphOrder"

        var sparkSession: SparkSession = null

        try {


            val startTime = System.nanoTime()

            sparkSession = SparkSessionFactory.createSession("Identification")
            println("\n" + "=" * 70)
            println(s"IDENTIFYING HIGHLY IRREGULAR SUBGRAPHS")
            println("=" * 70)

            findHighlyIrregularSubgraphs(
                n = subgraphOrder,
                subgraphsPath = subgraphsPath,
                targetGraphPath = targetGraphPath,
                outputPath = outputPath,
                spark = sparkSession
            )

            val endTime = System.nanoTime()
            val elapsedTime = (endTime - startTime) / 1e9d

            println(f"Time taken: $elapsedTime%.2f seconds")
            println("=" * 70)

        } finally {
            if (sparkSession != null) {
                SparkSessionFactory.stopSession(sparkSession)
            }
        }
    }
}
