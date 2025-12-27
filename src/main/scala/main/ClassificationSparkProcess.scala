package classification

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths}


object SparkProcess {

  import Utilities.{deleteDirectory, isHighlyIrregular}


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


  def classifyGraphs(inputDir: String, outputDir: String, spark: SparkSession): Unit = {
    import spark.implicits._

    val outputPath = Paths.get(outputDir)

    if (Files.exists(outputPath)) {
      deleteDirectory(outputPath.toFile)
    }
    Files.createDirectories(outputPath)

    val schema = StructType(Array(
      StructField("graph6", StringType, nullable = false),
      StructField("graph_order", IntegerType, nullable = false),
      StructField("edges", StringType, nullable = false)
    ))

    println()
    println("=" * 70)
    println(s"Reading graphs from $inputDir...")

    val df = spark.read
      .schema(schema)
      .parquet(inputDir)

    val n = df.select("graph_order").first().getInt(0)

    println(s"Classifying graphs of order n=$n...")

    val resultDF = df.withColumn(
      "is_highly_irregular",
      isHighlyIrregularUDF(col("edges"), col("graph_order"))
    )

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

    println(s"Creating vertices...")
    val verticesDF = highlyIrregularDF
      .select($"graph6")
      .flatMap { row =>
        val graph6 = row.getString(0)
        (0 until n).map { vertexId =>
          (vertexId, graph6)
        }
      }
      .toDF("id", "graph6")

    println(s"Creating edges...")
    val edgesDF = highlyIrregularDF
      .withColumn("edge_array", parseEdgesUDF($"edges"))
      .select($"graph6", explode($"edge_array").as("edge"))
      .select(
        $"graph6",
        $"edge._1".as("src_vertex"),
        $"edge._2".as("dst_vertex")
      )

    verticesDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "none")
      .parquet(s"$outputDir/vertices")

    edgesDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "none")
      .parquet(s"$outputDir/edges")

    println()
    println("=" * 70)
    println("Classification complete!")
    println(s"Results written to $outputDir")
    println("=" * 70)
  }


  def main(args: Array[String]): Unit = {
    val n = 10

    val inputDir = s"data/generated/order=$n"
    val outputDir = s"data/classified/order=$n"
    println(s"Input directory: $inputDir")
    println(s"Output directory: $outputDir")
    println()

    var sparkSession: SparkSession = null

    try {
      val startTime = System.nanoTime()

      sparkSession = SparkSessionFactory.createSession("Classification")

      classifyGraphs(inputDir, outputDir, sparkSession)

      val endTime = System.nanoTime()
      val elapsedTime = (endTime - startTime) / 1e9d

      println()
      println(f"Time taken: $elapsedTime%.2f seconds")

    } finally {
      if (sparkSession != null) {
        SparkSessionFactory.stopSession(sparkSession)
      }
    }
  }
}