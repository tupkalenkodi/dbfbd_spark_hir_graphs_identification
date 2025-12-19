package classification_of_highly_irregular

import com.sun.management.OperatingSystemMXBean
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.lang.management.ManagementFactory

object SparkSessionFactory {

  // Get system resources (Scala equivalent of psutil)
  private def checkSystemResources(): (Long, Long, Int, Int) = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]

    val totalMemory = osBean.getTotalMemorySize / (1024 * 1024 * 1024)  // GB
    val freeMemory = osBean.getFreeMemorySize / (1024 * 1024 * 1024)    // GB
    val logicalCores = osBean.getAvailableProcessors
    val physicalCores = Runtime.getRuntime.availableProcessors()  // Approximation

    (totalMemory, freeMemory, logicalCores, physicalCores)
  }

  // Create optimized Spark session
  def createOptimizedSession(appName: String): SparkSession = {
    val (totalMemory, availableMemory, cpuLogical, cpuPhysical) = checkSystemResources()

    println("=" * 60)
    println("System Resources Analysis:")
    println(s"  Total Memory: ${totalMemory}GB")
    println(s"  Available Memory: ${availableMemory}GB")
    println(s"  Logical Cores: $cpuLogical")
    println(s"  Physical Cores: $cpuPhysical")
    println("=" * 60)

    // Calculate memory allocation (70% of available memory)
    val usableMemory = (availableMemory * 0.7).toInt
    val driverMemory = Math.max(2, (usableMemory * 0.6).toInt)    // 60% for driver
    val executorMemory = Math.max(1, (usableMemory * 0.3).toInt)  // 30% for executor

    // Use physical cores, leave 1 for OS
    val executorCores = Math.max(1, cpuPhysical - 1)

    // Set parallelism based on cores
    val parallelism = executorCores * 2

    println(s"Spark Configuration:")
    println(s"  Driver Memory: ${driverMemory}GB")
    println(s"  Executor Memory: ${executorMemory}GB")
    println(s"  Executor Cores: $executorCores")
    println(s"  Parallelism: $parallelism")
    println("=" * 60)

    // Create Spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")  // For local mode
      // Memory settings
      .set("spark.driver.memory", s"${driverMemory}g")
      .set("spark.executor.memory", s"${executorMemory}g")
      .set("spark.executor.cores", executorCores.toString)
      .set("spark.driver.maxResultSize", s"${Math.max(1, driverMemory / 2)}g")
      // Memory management
      .set("spark.memory.fraction", "0.7")
      .set("spark.memory.storageFraction", "0.3")
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
      .set("spark.kryoserializer.buffer.max", "512m")
      // SQL optimizations
      .set("spark.sql.autoBroadcastJoinThreshold", "10485760")  // 10MB
      .set("spark.sql.broadcastTimeout", "300")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
      // Parquet optimizations (available in Spark 4.0)
      .set("spark.sql.parquet.enableVectorizedReader", "true")
      .set("spark.sql.parquet.columnarReaderBatchSize", "4096")
      // Dynamic allocation (for YARN cluster mode)
      .set("spark.dynamicAllocation.enabled", "false")  // false for local mode

    // Create Spark session
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // Set log level
    spark.sparkContext.setLogLevel("ERROR")

    println("Spark Session created successfully!")
    println(s"Spark Version: ${spark.version}")
    println("=" * 60)

    spark
  }

  // Alternative: Simple session creator for quick testing
  def createSimpleSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
  }

  // Stop session with cleanup
  def stopSession(spark: SparkSession): Unit = {
    try {
      spark.stop()
      println("Spark session stopped successfully.")
    } catch {
      case e: Exception =>
        println(s"Error stopping Spark session: ${e.getMessage}")
    }
  }
}