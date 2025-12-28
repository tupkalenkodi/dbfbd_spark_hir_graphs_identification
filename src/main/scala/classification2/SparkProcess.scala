package classification2

import org.apache.spark.sql.functions.{col, udf, explode, lit, concat}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}


object SparkProcess {

  import Utilities.isHighlyIrregular
  import Utilities.deleteDirectory


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

  // Classification
  def classifyGraphs(inputDir: String, outputDir: String, spark: SparkSession): Unit = {
    import spark.implicits._

    val outputPath = Paths.get(outputDir)

    // Clear output directory
    if (Files.exists(outputPath)) {
      deleteDirectory(outputPath.toFile)
    }
    Files.createDirectories(outputPath)

    // Define schema for input data
    val schema = StructType(Array(
      StructField("graph6", StringType, nullable = false),
      StructField("graph_order", IntegerType, nullable = false),
      StructField("edges", StringType, nullable = false)
    ))

    println()
    println("=" * 70)
    println(s"Reading graphs from $inputDir...")

    // Read parquet files from the order=n directory
    val df = spark.read
      .schema(schema)
      .parquet(inputDir)

    // Get the order from the first row (all rows should have same order)
    val n = df.select("graph_order").first().getInt(0)

    println(s"Classifying graphs of order n=$n...")

    // Classify graphs
    val resultDF = df.withColumn(
      "is_highly_irregular",
      isHighlyIrregularUDF(col("edges"), col("graph_order"))
    )

    // Filter highly irregular graphs
    val highlyIrregularDF = resultDF
      .filter(col("is_highly_irregular") === true)
      .select("graph6", "graph_order", "edges")

    val count = highlyIrregularDF.count()
    println(f"Found $count%,d highly irregular graphs of order $n")

    if (count == 0) {
      println("No highly irregular graphs found.")
      println("=" * 70)
      return
    }

    // Create vertices DataFrame
    // Each graph contributes n vertices, labeled as "graph6_vertexId"
    println(s"Creating vertices...")
    val verticesDF = highlyIrregularDF
      .select($"graph6")
      .flatMap { row =>
        val graph6 = row.getString(0)
        (0 until n).map { vertexId =>
          (s"${graph6}_$vertexId", graph6)
        }
      }
      .toDF("id", "graph6")

    // Create edges DataFrame
    // Parse edges and create vertex IDs
    println(s"Creating edges...")
    val edgesDF = highlyIrregularDF
      .withColumn("edge_array", parseEdgesUDF($"edges"))
      .select($"graph6", explode($"edge_array").as("edge"))
      .select(
        $"graph6",
        $"edge._1".as("src_vertex"),
        $"edge._2".as("dst_vertex")
      )
      .select(
        concat(lit(""), $"graph6", lit("_"), $"src_vertex").as("src"),
        concat(lit(""), $"graph6", lit("_"), $"dst_vertex").as("dst"),
        $"graph6"
      )

    // Save GraphFrame components
    verticesDF.write
      .mode("overwrite")
      .parquet(s"$outputDir/vertices")

    edgesDF.write
      .mode("overwrite")
      .parquet(s"$outputDir/edges")


    println()
    println("=" * 70)
    println("Classification complete!")
    println(s"Results written to $outputDir")
    println("=" * 70)
  }
}