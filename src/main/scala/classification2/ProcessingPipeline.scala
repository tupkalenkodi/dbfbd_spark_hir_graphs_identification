package classification

import com.github.mjakubowski84.parquet4s.{ParquetReader, Path}
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.collection.mutable


object ProcessingPipeline {
  // Case class for reading results
  case class HighlyIrregularGraph(graph6: String)

  // Main method
  def main(args: Array[String]): Unit = {
    val inputData = "data/chi/input_data"
    val sparkOutputDir = "data/chi/output_data/spark/scaling_factor=1"
    val simpleOutputDir = "data/chi/output_data/simple/scaling_factor=1"

    println()
    println("╔" + "═" * 68 + "╗")
    println("║" + " " * 20 + "PROCESSING PIPELINE" + " " * 29 + "║")
    println("╚" + "═" * 68 + "╝")
    println()

    var sparkElapsedTime: Double = 0.0
    var simpleElapsedTime: Double = 0.0

    // SPARK PROCESS
    println("Running Spark Process...")
    println("-" * 70)

    val sparkStartTime = System.nanoTime()
    var sparkSession: SparkSession = null

    try {
      sparkSession = SparkSessionFactory.createOptimizedSession("ScalaGraphClassifier")
      SparkProcess.classifyGraphs(inputData, sparkOutputDir, sparkSession)

      val sparkEndTime = System.nanoTime()
      sparkElapsedTime = (sparkEndTime - sparkStartTime) / 1e9d

      println("=" * 70)
      println(f"Spark processing complete! Time taken: $sparkElapsedTime%.2f seconds")
      println("=" * 70)
      println()

    } finally {
      if (sparkSession != null) {
        SparkSessionFactory.stopSession(sparkSession)
      }
    }

    // SIMPLE PROCESS
    println("Running Simple Process...")
    println("-" * 70)

    val simpleStartTime = System.nanoTime()

    SimpleProcess.classifyGraphs(inputData, simpleOutputDir)

    val simpleEndTime = System.nanoTime()
    simpleElapsedTime = (simpleEndTime - simpleStartTime) / 1e9d

    println("=" * 70)
    println(f"Simple processing complete! Time taken: $simpleElapsedTime%.2f seconds")
    println("=" * 70)
    println()

    // COMPARE OUTPUTS
    println("Comparing outputs...")

    val outputsMatch = compareOutputs(sparkOutputDir, simpleOutputDir)

    println("=" * 70)
    if (outputsMatch) {
      println("OKAY - Both processes produced identical results!")
      var speedup = sparkElapsedTime / simpleElapsedTime

      println("\n PERFORMANCE SUMMARY:")
      println("-" * 70)
      println(f"Simple Scala: $simpleElapsedTime%.2f seconds")
      println(f"Spark Scala:  $sparkElapsedTime%.2f seconds")
      if (speedup > 1) {
        println(f"Simple is faster by $speedup%.2f times")
      } else {
        speedup = 1 / speedup
        println(f"Spark is faster by $speedup%.2f times")
      }

      println("-" * 70)

    } else {
      println("MISMATCH - Outputs differ between processes!")
    }
    println()
  }

  // Compare outputs from both processes
  private def compareOutputs(sparkDir: String, simpleDir: String): Boolean = {
    val sparkGraphs = readParquetResults(sparkDir)
    val simpleGraphs = readParquetResults(simpleDir)

    val sparkSet = sparkGraphs.toSet
    val simpleSet = simpleGraphs.toSet

    if (sparkSet == simpleSet) {
      true
    } else {
      false
    }
  }

  // Read all graph6 strings from parquet files in a directory
  private def readParquetResults(directory: String): Seq[String] = {
    val dir = new File(directory)

    if (!dir.exists() || !dir.isDirectory) {
      return Seq.empty
    }

    val parquetFiles = findParquetFiles(dir)
    val graphs = mutable.ArrayBuffer[String]()

    parquetFiles.foreach { filePath =>
      ParquetReader.as[HighlyIrregularGraph].read(Path(filePath)).foreach { record =>
        graphs += record.graph6
      }
    }

    graphs.toSeq
  }

  // Find all parquet files in directory recursively
  private def findParquetFiles(directory: File): Seq[String] = {
    def recurse(file: File): Seq[File] = {
      if (file.isDirectory) {
        Option(file.listFiles())
          .map(_.flatMap(recurse).toSeq)
          .getOrElse(Seq.empty)
      } else if (file.getName.endsWith(".parquet")) {
        Seq(file)
      } else {
        Seq.empty
      }
    }

    recurse(directory).map(_.getAbsolutePath)
  }
}