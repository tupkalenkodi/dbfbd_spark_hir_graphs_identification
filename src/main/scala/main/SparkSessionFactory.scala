package main

import com.sun.management.OperatingSystemMXBean
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}


object SparkSessionFactory {

  // Get system resources (Scala equivalent of psutil)
  private def checkSystemResources(): (Long, Long, Int) = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]

    val totalMemory = osBean.getTotalMemorySize / (1024 * 1024 * 1024)  // GB
    val freeMemory = osBean.getFreeMemorySize / (1024 * 1024 * 1024)    // GB
    val logicalCores = osBean.getAvailableProcessors

    (totalMemory, freeMemory, logicalCores)
  }

  // Create optimized Spark session
  def createSession(appName: String): SparkSession = {
    val (totalMemory, availableMemory, cpuLogical) = checkSystemResources()

    // Calculate memory allocation (70% of available memory)
    val usableMemory = (availableMemory * 0.9).toInt
    val driverMemory = Math.max(2, (usableMemory * 0.7).toInt)    // 60% for driver
    val executorMemory = Math.max(1, (usableMemory * 0.3).toInt)  // 30% for executor

    // Use physical cores, leave 1 for OS
    val executorCores = Math.max(1, cpuLogical - 1)

    // Set parallelism based on cores
    val parallelism = executorCores * 2

    // Create Spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")  // For local mode
      // Memory settings
      .set("spark.driver.memory", s"${driverMemory}g")
      .set("spark.executor.memory", s"${executorMemory}g")
      .set("spark.executor.cores", executorCores.toString)
      // Memory management
      .set("spark.memory.fraction", "1")
      // Performance optimizations
      .set("spark.sql.files.maxPartitionBytes", "134217728")  // 128MB
      .set("spark.default.parallelism", parallelism.toString)
      .set("spark.sql.shuffle.partitions", parallelism.toString)
      // Adaptive query execution
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .set("spark.sql.adaptive.skewJoin.enabled", "true")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
      // Serialization
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      // SQL optimizations
      .set("spark.sql.autoBroadcastJoinThreshold", "10485760")  // 10MB
      .set("spark.sql.broadcastTimeout", "300")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
      // Parquet optimizations (available in Spark 4.0)
      .set("spark.sql.parquet.enableVectorizedReader", "true")
      .set("spark.sql.parquet.columnarReaderBatchSize", "4096")

    // Create Spark session
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // Set log level
    spark.sparkContext.setLogLevel("ERROR")

    // Setup checkpoint directory for GraphFrames and iterative algorithms
    val checkpointDir = "/tmp/spark-checkpoint"
    val checkpointPath = Paths.get(checkpointDir)

    // Create checkpoint directory if it doesn't exist
    if (!Files.exists(checkpointPath)) {
      Files.createDirectories(checkpointPath)
    }

    spark.sparkContext.setCheckpointDir(checkpointDir)

    spark
  }

  // Stop session with cleanup
  def stopSession(spark: SparkSession): Unit = {
    try {
      spark.stop()
    } catch {
      case e: Exception =>
        println(s"Error stopping Spark session: ${e.getMessage}")
    }
  }
}