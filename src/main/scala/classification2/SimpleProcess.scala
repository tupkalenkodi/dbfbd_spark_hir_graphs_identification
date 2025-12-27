package classification

import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path}
import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._
import java.util.concurrent.atomic.AtomicInteger


object SimpleProcess {

  import Utilities.isHighlyIrregular
  import Utilities.deleteDirectory

  // Input case class matching the parquet schema
  case class GraphRecord(
                          graph6: String,
                          graph_order: Int,
                          size: Int,
                          edges: String
                        )

  // Output case class
  case class HighlyIrregularGraph(graph6: String)

  // Find all parquet files in directory recursively
  private def findParquetFiles(directory: String): Seq[String] = {
    val dir = new File(directory)

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

    recurse(dir).map(_.getAbsolutePath)
  }

  // Process a single parquet file
  private def processParquetFile(filePath: String): Map[Int, Seq[String]] = {
    val highlyIrregular = mutable.Map[Int, mutable.ArrayBuffer[String]]()

    ParquetReader.as[GraphRecord].read(Path(filePath)).foreach { record =>
      if (isHighlyIrregular(record.edges, record.graph_order)) {
        highlyIrregular
          .getOrElseUpdate(record.graph_order, mutable.ArrayBuffer.empty) += record.graph6
      }
    }

    highlyIrregular.view.mapValues(_.toSeq).toMap
  }

  // Merge results from parallel processing
  private def mergeResults(allResults: Seq[Map[Int, Seq[String]]]): Map[Int, Seq[String]] = {
    val merged = mutable.Map[Int, mutable.ArrayBuffer[String]]()

    allResults.foreach { result =>
      result.foreach { case (order, graphs) =>
        merged.getOrElseUpdate(order, mutable.ArrayBuffer.empty) ++= graphs
      }
    }

    merged.view.mapValues(_.toSeq).toMap
  }

  private def writeResults(outputDir: String, mergedResults: Map[Int, Seq[String]]): Unit = {
    val outputPath = Paths.get(outputDir)

    // Clear output directory
    if (Files.exists(outputPath)) {
      deleteDirectory(outputPath.toFile)
    }
    Files.createDirectories(outputPath)

    // Create a single output file
    val outputFile = outputPath.resolve("data.parquet").toString

    // Collect all graphs into a single sequence
    val allGraphs = mergedResults.values.flatten.map(HighlyIrregularGraph)

    ParquetWriter.of[HighlyIrregularGraph].writeAndClose(Path(outputFile), allGraphs)
  }

  // Main classification function
  def classifyGraphs(inputDir: String, outputDir: String): Unit = {
    println()
    println("=" * 70)
    println(s"Reading graphs from $inputDir...")

    // Find all parquet files
    val parquetFiles = findParquetFiles(inputDir)

    println("Processing graphs...")

    // Process files in parallel
    val processedCounter = new AtomicInteger(0)

    val results =
      parquetFiles.par.map { file =>
        val result = processParquetFile(file)
        val count = processedCounter.incrementAndGet()
        println(s"  [$count/${parquetFiles.size}] Completed")
        result
      }.seq

    println("=" * 70)

    // Merge results
    val mergedResults = mergeResults(results)
    val totalCount = mergedResults.values.map(_.size).sum

    println(f"Found $totalCount%,d highly irregular graph(s)")

    // Write results
    if (totalCount > 0) {
      writeResults(outputDir, mergedResults)
      println(s"Results written to $outputDir")
    } else {
      println("No highly irregular graphs found.")
    }
  }
}
