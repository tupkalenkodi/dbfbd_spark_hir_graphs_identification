package classification_of_highly_irregular

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}


object SparkProcess {

  import Utilities.isHighlyIrregular
  import Utilities.deleteDirectory


  // Create UDF for Spark that accepts string edges
  private val isHighlyIrregularUDF: org.apache.spark.sql.expressions.UserDefinedFunction =
    udf((edgesStr: String, order: Int) => {
      isHighlyIrregular(edgesStr, order)
    })


  // Main classification function
  def classifyGraphs(inputDir: String, outputDir: String, spark: SparkSession): Unit = {
    val outputPath = Paths.get(outputDir)

    // Clear output directory
    if (Files.exists(outputPath)) {
      deleteDirectory(outputPath.toFile)
    }
    Files.createDirectories(outputPath)

    // Define schema for the input data
    val schema = StructType(Array(
      StructField("graph6", StringType, nullable = false),
      StructField("graph_order", IntegerType, nullable = false),
      StructField("size", IntegerType, nullable = false),
      StructField("edges", StringType, nullable = false)
    ))

    println()
    println("=" * 70)
    println(s"Reading graphs from $inputDir...")

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
      .select("graph6")

    val count = highlyIrregularDF.count()
    println(f"Found $count%,d highly irregular graphs")

    // Write results
    if (count > 0) {
      highlyIrregularDF.write
        .mode("overwrite")
        .parquet(outputDir)

      println(s"Results written to $outputDir")
    } else {
      println("No highly irregular graphs found.")
    }
  }
}
