//package identification
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//
//import scala.collection.mutable
//
//object SparkProcess {
//
//  /**
//   * Main function to find all highly irregular subgraph matches in a large graph
//   *
//   * @param n Order of the highly irregular graphs to find
//   * @param subgraphsPath Path to parquet files containing highly irregular graphs
//   * @param bigGraphPath Path to the large graph (vertices and edges parquet)
//   * @param outputPath Where to save results
//   * @param spark SparkSession
//   */
//  def findHighlyIrregularSubgraphs(
//                                    n: Int,
//                                    subgraphsPath: String,
//                                    bigGraphPath: String,
//                                    outputPath: String,
//                                    spark: SparkSession
//                                  ): Unit = {
//
//    println(s"=" * 70)
//    println(s"Finding highly irregular subgraphs of order $n")
//    println(s"Subgraphs path: $subgraphsPath")
//    println(s"Big graph path: $bigGraphPath")
//    println(s"=" * 70)
//
//    // Step 1: Load pattern graphs (highly irregular graphs)
//    println("\n[1] Loading pattern graphs...")
//    val patterns = loadPatternGraphs(subgraphsPath, spark)
//    val patternCount = patterns.length
//    println(s"    Loaded $patternCount highly irregular graphs of order $n")
//
//    if (patternCount == 0) {
//      println("    ERROR: No pattern graphs found!")
//      return
//    }
//
//    // Step 2: Load the big graph
//    println("\n[2] Loading target graph...")
//    val bigGraph = loadBigGraph(bigGraphPath, spark)
//
//    // Step 3: Find connected components of order >= n
//    println("\n[3] Finding connected components...")
//    val validComponents = findValidComponents(bigGraph, n, spark)
//
//    if (validComponents.isEmpty) {
//      println("    No valid components found!")
//      return
//    }
//
//    // Step 4: For each component, find all pattern matches
//    println("\n[4] Searching for subgraph matches...")
//    var totalMatches = 0L
//
//    patterns.zipWithIndex.foreach { case (pattern, pidx) =>
//      println(s"      Pattern ${pidx + 1}/$patternCount...")
//
//      validComponents.zipWithIndex.foreach { case (component, cidx) =>
//        val matches = findSubgraphMatches(component, pattern, n, spark)
//
//        if (matches.nonEmpty) {
//          totalMatches += matches.length
//
//          // Save matches for this component and pattern
//          saveMatches(matches, s"$outputPath/component_$cidx/pattern_$pidx", spark)
//        }
//      }
//    }
//
//    println("\n" + "=" * 80)
//    println(s"TOTAL MATCHES FOUND: $totalMatches")
//    println(s"Results saved to: $outputPath")
//    println("=" * 80)
//  }
//
//  // Load pattern graphs from graph6 format
//  private def loadPatternGraphs(path: String, spark: SparkSession): Array[PatternGraph] = {
//
//    val df = spark.read.parquet(path)
//
//    df.select("graph6", "edges").collect().map { row =>
//      val graph6 = row.getString(0)
//      val edgesStr = row.getString(1)
//      PatternGraph.fromEdgeString(graph6, edgesStr)
//    }
//  }
//
//  // Load the big graph as a GraphFrame
//  private def loadBigGraph(path: String, spark: SparkSession): GraphFrame = {
//    val vertices = spark.read.parquet(s"$path/vertices")
//    val edges = spark.read.parquet(s"$path/edges")
//    GraphFrame(vertices, edges)
//  }
//
//  // Find connected components with at least minOrder vertices
//  private def findValidComponents(
//                                   graph: GraphFrame,
//                                   minOrder: Int,
//                                   spark: SparkSession
//                                 ): Array[GraphFrame] = {
//    import spark.implicits._
//
//    // Find connected components
//    val components = graph.connectedComponents.run()
//
//    // Count vertices per component
//    val componentSizes = components
//      .groupBy("component")
//      .agg(count("*").as("size"))
//      .filter($"size" >= minOrder)
//
//    // Get list of valid component IDs
//    val validComponentIds = componentSizes
//      .select("component")
//      .as[Long]
//      .collect()
//
//    // Create a GraphFrame for each valid component
//    validComponentIds.map { componentId =>
//      val componentVertices = components
//        .filter($"component" === componentId)
//        .join(graph.vertices, "id")
//
//      val componentVertexIds = componentVertices.select("id").as[String].collect().toSet
//      val componentVertexIdsBroadcast = spark.sparkContext.broadcast(componentVertexIds)
//
//      val componentEdges = graph.edges.filter { row =>
//        val src = row.getAs[String]("src")
//        val dst = row.getAs[String]("dst")
//        componentVertexIdsBroadcast.value.contains(src) &&
//          componentVertexIdsBroadcast.value.contains(dst)
//      }
//
//      GraphFrame(componentVertices.select(graph.vertices.columns.map(col): _*), componentEdges)
//    }
//  }
//
//  // Find all subgraph matches using VF2 algorithm
//  private def findSubgraphMatches(
//                                   targetGraph: GraphFrame,
//                                   pattern: PatternGraph,
//                                   n: Int,
//                                   spark: SparkSession
//                                 ): Array[SubgraphMatch] = {
//
//    // Collect target graph to driver
//    val targetVertices = targetGraph.vertices.select("id").as[String].collect()
//    val targetEdges = targetGraph.edges
//      .select("src", "dst")
//      .collect()
//      .flatMap { row =>
//        val src = row.getString(0)
//        val dst = row.getString(1)
//        Seq((src, dst), (dst, src)) // Make undirected
//      }
//      .toSet
//
//    // Build graph structures
//    val targetG = Graph.fromEdges(targetVertices, targetEdges)
//    val patternG = Graph.fromPattern(pattern, n)
//
//    // Run VF2 algorithm
//    val vf2 = new VF2Matcher(patternG, targetG)
//    val matches = vf2.findAllMatches()
//
//    matches.map(mapping => SubgraphMatch(pattern.graph6, mapping)).toArray
//  }
//
//  // Save matches to parquet
//  private def saveMatches(
//                           matches: Array[SubgraphMatch],
//                           path: String,
//                           spark: SparkSession
//                         ): Unit = {
//    import spark.implicits._
//
//    val matchesDF = matches.map { m =>
//      (m.patternId, m.mapping.map { case (k, v) => s"$k->$v" }.mkString(";"))
//    }.toSeq.toDF("pattern_id", "vertex_mapping")
//
//    matchesDF.write.mode("overwrite").parquet(path)
//  }
//}
//
//// Case class representing a pattern graph
//case class PatternGraph(
//                         graph6: String,
//                         edges: Set[(Int, Int)],
//                         degrees: Map[Int, Int],
//                         maxDegree: Int
//                       )
//
//object PatternGraph {
//  def fromEdgeString(graph6: String, edgesStr: String): PatternGraph = {
//    val edges = if (edgesStr.isEmpty) {
//      Set.empty[(Int, Int)]
//    } else {
//      edgesStr.split(";").map { edge =>
//        val parts = edge.split(",")
//        (parts(0).toInt, parts(1).toInt)
//      }.toSet
//    }
//
//    // Compute degrees
//    val degreeMap = mutable.Map[Int, Int]()
//    edges.foreach { case (u, v) =>
//      degreeMap(u) = degreeMap.getOrElse(u, 0) + 1
//      degreeMap(v) = degreeMap.getOrElse(v, 0) + 1
//    }
//
//    val maxDeg = degreeMap.values.max
//
//    PatternGraph(graph6, edges, degreeMap.toMap, maxDeg)
//  }
//}
//
//// Case class representing a subgraph match
//case class SubgraphMatch(
//                          patternId: String,
//                          mapping: Map[Int, String]  // pattern vertex -> target vertex
//                        )
//
//// Simple graph representation for VF2
//case class Graph(
//                  vertices: Array[String],
//                  vertexToIndex: Map[String, Int],
//                  adjacency: Array[Set[Int]],
//                  degrees: Array[Int]
//                )
//
//object Graph {
//  def fromEdges(vertices: Array[String], edges: Set[(String, String)]): Graph = {
//    val vertexToIdx = vertices.zipWithIndex.toMap
//    val n = vertices.length
//    val adj = Array.fill(n)(Set.empty[Int])
//
//    edges.foreach { case (u, v) =>
//      val uIdx = vertexToIdx(u)
//      val vIdx = vertexToIdx(v)
//      adj(uIdx) = adj(uIdx) + vIdx
//    }
//
//    val degs = adj.map(_.size)
//    Graph(vertices, vertexToIdx, adj, degs)
//  }
//
//  def fromPattern(pattern: PatternGraph, n: Int): Graph = {
//    val vertices = (0 until n).map(_.toString).toArray
//    val vertexToIdx = vertices.zipWithIndex.toMap
//    val adj = Array.fill(n)(Set.empty[Int])
//
//    pattern.edges.foreach { case (u, v) =>
//      adj(u) = adj(u) + v
//      adj(v) = adj(v) + u
//    }
//
//    val degs = adj.map(_.size)
//    Graph(vertices, vertexToIdx, adj, degs)
//  }
//}
//
///**
// * VF2 Algorithm for subgraph isomorphism
// * Based on: "A (Sub)Graph Isomorphism Algorithm for Matching Large Graphs"
// * by Cordella, Foggia, Sansone, and Vento (2004)
// */
//class VF2Matcher(pattern: Graph, target: Graph) {
//
//  private val patternSize = pattern.vertices.length
//  private val targetSize = target.vertices.length
//
//  // State for the matching process
//  private class State(
//                       val core1: Array[Int],  // pattern -> target mapping (-1 if unmapped)
//                       val core2: Array[Int],  // target -> pattern mapping (-1 if unmapped)
//                       val depth: Int
//                     ) {
//    def isMapped(patternVertex: Int): Boolean = core1(patternVertex) != -1
//    def isUsed(targetVertex: Int): Boolean = core2(targetVertex) != -1
//
//    def copy(): State = new State(core1.clone(), core2.clone(), depth)
//  }
//
//  // Find all subgraph isomorphisms
//  def findAllMatches(): Array[Map[Int, String]] = {
//    val matches = mutable.ArrayBuffer[Map[Int, String]]()
//    val initialState = new State(
//      Array.fill(patternSize)(-1),
//      Array.fill(targetSize)(-1),
//      0
//    )
//
//    match(initialState, matches)
//    matches.toArray
//  }
//
//  // Recursive matching function
//  private def match(state: State, matches: mutable.ArrayBuffer[Map[Int, String]]): Unit = {
//    if (state.depth == patternSize) {
//      // Found a complete match
//      val mapping = state.core1.zipWithIndex.map { case (targetIdx, patternIdx) =>
//        patternIdx -> target.vertices(targetIdx)
//      }.toMap
//      matches += mapping
//      return
//    }
//
//    // Generate candidate pairs
//    val candidates = generateCandidatePairs(state)
//
//    candidates.foreach { case (patternVertex, targetVertex) =>
//      if (isFeasible(state, patternVertex, targetVertex)) {
//        // Add this pair to the state
//        val newState = state.copy()
//        newState.core1(patternVertex) = targetVertex
//        newState.core2(targetVertex) = patternVertex
//
//        val nextState = new State(newState.core1, newState.core2, state.depth + 1)
//        match(nextState, matches)
//      }
//    }
//  }
//
//  // Generate candidate pairs for matching
//  private def generateCandidatePairs(state: State): Seq[(Int, Int)] = {
//    // Find first unmapped pattern vertex
//    val patternVertex = (0 until patternSize).find(p => !state.isMapped(p)).getOrElse(-1)
//
//    if (patternVertex == -1) return Seq.empty
//
//    // Generate candidates based on degree and neighborhood constraints
//    val patternDegree = pattern.degrees(patternVertex)
//
//    (0 until targetSize)
//      .filter(t => !state.isUsed(t))
//      .filter(t => target.degrees(t) >= patternDegree) // Degree constraint
//      .map(t => (patternVertex, t))
//  }
//
//  // Check if a candidate pair is feasible (VF2 feasibility rules)
//  private def isFeasible(state: State, patternVertex: Int, targetVertex: Int): Boolean = {
//    // Rule 1: Check degree constraint
//    if (target.degrees(targetVertex) < pattern.degrees(patternVertex)) {
//      return false
//    }
//
//    // Rule 2: Check consistency with existing mappings (look-ahead rules)
//    // For each neighbor of patternVertex
//    pattern.adjacency(patternVertex).foreach { patternNeighbor =>
//      if (state.isMapped(patternNeighbor)) {
//        // If this neighbor is already mapped, the target vertex must be connected to its mapping
//        val mappedTarget = state.core1(patternNeighbor)
//        if (!target.adjacency(targetVertex).contains(mappedTarget)) {
//          return false
//        }
//      }
//    }
//
//    // Rule 3: For mapped pattern vertices, check reverse edges
//    (0 until patternSize).foreach { p =>
//      if (state.isMapped(p)) {
//        val t = state.core1(p)
//        val patternHasEdge = pattern.adjacency(patternVertex).contains(p)
//        val targetHasEdge = target.adjacency(targetVertex).contains(t)
//
//        if (patternHasEdge != targetHasEdge) {
//          return false
//        }
//      }
//    }
//
//    // Rule 4: Look-ahead counting rules
//    // Count unmapped neighbors in pattern
//    val patternUnmappedNeighbors = pattern.adjacency(patternVertex)
//      .count(n => !state.isMapped(n))
//
//    // Count unmapped neighbors in target
//    val targetUnmappedNeighbors = target.adjacency(targetVertex)
//      .count(n => !state.isUsed(n))
//
//    // Target must have at least as many unmapped neighbors
//    if (targetUnmappedNeighbors < patternUnmappedNeighbors) {
//      return false
//    }
//
//    // Rule 5: Semantic feasibility (degree in induced subgraph)
//    // For highly irregular graphs, we can add stricter degree sequence checks
//    if (!checkDegreeSequenceFeasibility(state, patternVertex, targetVertex)) {
//      return false
//    }
//
//    true
//  }
//
//  // Additional constraint: check if degree sequence is compatible
//  // This is particularly important for highly irregular graphs
//  private def checkDegreeSequenceFeasibility(
//                                              state: State,
//                                              patternVertex: Int,
//                                              targetVertex: Int
//                                            ): Boolean = {
//    // For each mapped pattern vertex, check if the induced degrees match
//    val patternMappedNeighbors = pattern.adjacency(patternVertex)
//      .count(n => state.isMapped(n))
//
//    val targetMappedNeighbors = target.adjacency(targetVertex)
//      .count(n => state.isUsed(n))
//
//    // The number of already-mapped neighbors must match exactly
//    patternMappedNeighbors == targetMappedNeighbors
//  }
//}
