from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType, StructType, StructField, ArrayType, IntegerType, StringType
import create_spark_session
import time


def create_graph_from_edges(spark, edges_df, order):
    """
    Create a GraphFrame from edges DataFrame for a single graph.

    Args:
        spark: SparkSession
        edges_df: DataFrame with columns ['src', 'dst']
        order: Number of vertices in the graph

    Returns:
        GraphFrame: GraphFrame representing the graph
    """
    # Create vertices DataFrame (all vertices from 0 to order-1)
    vertices_data = [(i,) for i in range(order)]
    vertices_df = spark.createDataFrame(vertices_data, ["id"])

    # Create GraphFrame
    return GraphFrame(vertices_df, edges_df)


def is_highly_irregular(gf):
    process the GraphFrame


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