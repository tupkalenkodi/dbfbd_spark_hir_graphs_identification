import psutil
from pyspark.sql import SparkSession

def check_system_resources():
    total_memory = psutil.virtual_memory().total / (1024 ** 3)
    available_memory = psutil.virtual_memory().available / (1024 ** 3)

    return (total_memory, available_memory,
            psutil.cpu_count(logical=True), psutil.cpu_count(logical=False))


def main(app_name):
    total_memory, available_memory, cpu_logical, cpu_physical = check_system_resources()

    # CALCULATE MEMORY ALLOCATION (use 70% of available memory, 30% for OS)
    usable_memory = int(available_memory * 0.7)
    driver_memory = max(2, int(usable_memory * 0.6))  # 60% for driver
    executor_memory = max(1, int(usable_memory * 0.3))  # 30% for executor

    # USE PHYSICAL CORES, LEAVE 1 FOR OS
    executor_cores = max(1, cpu_physical - 1)

    # SET PARALLELISM BASED ON CORES
    parallelism = executor_cores * 2

    config = {
        "spark.driver.memory": f"{driver_memory}g",
        "spark.executor.memory": f"{executor_memory}g",
        "spark.executor.cores": str(executor_cores),
        "spark.driver.maxResultSize": f"{max(1, driver_memory // 2)}g",
        "spark.memory.fraction": "0.7",
        "spark.memory.storageFraction": "0.3",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.sql.parquet.columnarReaderBatchSize": "4096",
        "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
        "spark.default.parallelism": str(parallelism),
        "spark.sql.shuffle.partitions": str(parallelism),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "512m",
    }

    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for key, value in config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark