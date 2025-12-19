package classification_of_highly_irregular

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer


object GraphClassifier {

  // Import the session factory
  import SparkSessionFactory.createOptimizedSession

  // Function to compute neighbor degrees
  def computeNeighborDegrees(edges: Seq[Seq[Int]], order: Int): Seq[Seq[Int]] = {

    // Initialize adjacency list and degree array
    val adjList = Array.fill(order)(ArrayBuffer.empty[Int])
    val degree = Array.fill(order)(0)

    // Build adjacency list and compute degrees
    edges.foreach { edge =>
      if (edge != null && edge.size >= 2) {
        val u = edge(0)
        val v = edge(1)

        // Bounds check
        if (u >= 0 && u < order && v >= 0 && v < order) {
          adjList(u) += v
          adjList(v) += u
          degree(u) += 1
          degree(v) += 1
        }
      }
    }

    // Compute neighbor degrees for each vertex
    (0 until order).map { v =>
      adjList(v).map(u => degree(u)).toSeq
    }.toSeq
  }

  // Function to check if graph is highly irregular
  def isHighlyIrregular(edges: Seq[Seq[Int]], order: Int): Boolean = {
    if (edges == null || edges.isEmpty || order <= 0) return false

    val neighborDegreesList = computeNeighborDegrees(edges, order)

    if (neighborDegreesList.isEmpty) return false

    // Check if highly irregular
    // For each vertex, all its neighbors must have distinct degrees
    neighborDegreesList.forall { vertexDegrees =>
      if (vertexDegrees.isEmpty) {
        // Isolated vertex - no neighbors, so trivially satisfies the condition
        true
      } else {
        // If vertex has neighbors with duplicate degrees, NOT highly irregular
        vertexDegrees.distinct.size == vertexDegrees.size
      }
    }
  }

  // Create UDF for Spark
  val isHighlyIrregularUDF = udf((edges: Seq[Seq[Int]], order: Int) => {
    isHighlyIrregular(edges, order)
  })

  // Main classification function
  def classifyGraphs(spark: SparkSession, inputDir: String, outputDir: String): DataFrame = {
    println()
    println("=" * 70)
    println(s"Reading graphs from $inputDir...")

    // Define schema for the input data
    val schema = StructType(Array(
      StructField("graph6", StringType, nullable = false),
      StructField("graph_order", IntegerType, nullable = false),
      StructField("size", IntegerType, nullable = false),
      StructField("edges", ArrayType(ArrayType(IntegerType)), nullable = false)
    ))

    // Read the parquet files
    val df = spark.read
      .schema(schema)
      .parquet(inputDir)

    println("Processing graphs...")

    // Apply the UDF to classify graphs
    val resultDF = df.withColumn(
      "is_highly_irregular",
      isHighlyIrregularUDF(col("edges"), col("graph_order"))
    )

    // Filter highly irregular graphs
    val highlyIrregularDF = resultDF
      .filter(col("is_highly_irregular") === true)
      .select("graph6", "graph_order")

    // Repartition by graph order for better performance
    val repartitionedDF = highlyIrregularDF.repartition(col("graph_order"))

    val count = repartitionedDF.count()
    println(f"Found $count%,d highly irregular graphs")

    if (count > 0) {
      // Write results to output directory
      repartitionedDF.write
        .mode("overwrite")
        .partitionBy("graph_order")
        .parquet(outputDir)

      println(s"Results written to $outputDir")
    } else {
      println("No highly irregular graphs found.")
    }

    repartitionedDF
  }

  // Main method
  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()

    // Create optimized Spark session
    val spark = createOptimizedSession("ScalaGraphClassifier")

    try {
      // Process graphs
      classifyGraphs(
        spark,
        "input_data",
        "spark_output_data"
      )

      val endTime = System.nanoTime()
      val elapsedTime = (endTime - startTime) / 1e9d

      println("=" * 70)
      println(f"Processing complete! Time taken: $elapsedTime%.2f seconds")
      println("=" * 70)

    } finally {
      // Stop session
      SparkSessionFactory.stopSession(spark)
    }
  }
}