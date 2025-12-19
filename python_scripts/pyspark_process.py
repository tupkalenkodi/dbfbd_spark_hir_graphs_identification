from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType, StructType, StructField, ArrayType, IntegerType, StringType
import create_spark_session
import time


def compute_neighbor_degrees(edges, order):
    if not edges:
        return []

    adj_list = [[] for _ in range(order)]
    degree = [0] * order

    # Build adjacency list and compute degrees
    for u, v in edges:
        adj_list[u].append(v)
        adj_list[v].append(u)
        degree[u] += 1
        degree[v] += 1

    # Compute neighbor degrees for each vertex
    neighbor_degrees = []
    for v in range(order):
        if adj_list[v]:  # Only compute if vertex has neighbors
            neighbors_deg = [degree[u] for u in adj_list[v]]
            neighbor_degrees.append(neighbors_deg)
        else:
            neighbor_degrees.append([])  # Isolated vertex

    return neighbor_degrees


def is_highly_irregular(edges, order):
    if not edges:
        return False

    # First compute neighbor degrees
    neighbor_degrees_list = compute_neighbor_degrees(edges, order)

    if not neighbor_degrees_list:
        return False

    # Check if highly irregular
    for vertex_degrees in neighbor_degrees_list:
        if not vertex_degrees:  # Isolated vertex, skip
            continue
        # IF ANY VERTEX HAS NEIGHBORS WITH DUPLICATE DEGREES, NOT HIGHLY IRREGULAR
        if len(vertex_degrees) != len(set(vertex_degrees)):
            return False
    return True

is_highly_irregular_udf = udf(is_highly_irregular, BooleanType())


def classify_graphs(spark, input_dir, output_dir):
    print('=' * 70)
    print(f"Reading graphs from {input_dir}...")

    schema = StructType([
        StructField("graph6", StringType(), nullable=False),
        StructField("graph_order", IntegerType(), nullable=False),
        StructField("size", IntegerType(), nullable=False),
        StructField("edges", ArrayType(ArrayType(IntegerType())), nullable=False)
    ])

    df = spark.read.schema(schema).parquet(f"{input_dir}")

    print("Processing graphs...")

    result_df = df.withColumn(
        "is_highly_irregular",
        is_highly_irregular_udf(col("edges"), col("graph_order"))
    )

    highly_irregular_df = result_df.filter(col("is_highly_irregular") == True)

    highly_irregular_df = highly_irregular_df.repartition("graph_order")

    count = highly_irregular_df.count()
    print(f"Found {count:,} highly irregular graphs")

    if count > 0:
        highly_irregular_df.write \
            .mode("overwrite") \
            .partitionBy("graph_order") \
            .parquet(f"{output_dir}")

        print(f"Results written to {output_dir}")
    else:
        print("No highly irregular graphs found.")

    return highly_irregular_df


def main():
    start_time = time.time()

    # CREATE SPARK SESSION
    spark = create_spark_session.main(app_name="GraphClassifier")

    try:
        classify_graphs(
            spark,
            'input_data',
            'pyspark_output_data'
        )

        end_time = time.time()
        elapsed_time = end_time - start_time

        print("=" * 70)
        print(f"Processing complete! Time taken: {elapsed_time:.2f} seconds")
        print("=" * 70)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()