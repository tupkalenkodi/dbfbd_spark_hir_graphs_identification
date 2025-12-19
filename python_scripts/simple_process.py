import pyarrow.parquet as pq
from pathlib import Path
from collections import defaultdict
from multiprocessing import Pool
import shutil
import psutil
import time



def is_highly_irregular(neighbor_degrees_list):
    if neighbor_degrees_list is None:
        return False

    for vertex_degrees in neighbor_degrees_list:
        if vertex_degrees is None:
            continue
        # IF ANY VERTEX HAS NEIGHBORS WITH DUPLICATE DEGREES, NOT HIGHLY IRREGULAR
        if len(vertex_degrees) != len(set(vertex_degrees)):
            return False
    return True


def process_graph_edges(edges, order):
    adj_list = [[] for _ in range(order)]
    degree = [0] * order

    for u, v in edges:
        adj_list[u].append(v)
        adj_list[v].append(u)
        degree[u] += 1
        degree[v] += 1

    neighbor_degrees = []
    for v in range(order):
        if adj_list[v]:
            neighbors_deg = [degree[u] for u in adj_list[v]]
            neighbor_degrees.append(neighbors_deg)
        else:
            neighbor_degrees.append([])

    return neighbor_degrees


def process_parquet_file(file_path):
    table = pq.read_table(file_path)

    graph6_list = table['graph6'].to_pylist()
    order_list = table['graph_order'].to_pylist()
    edges_list = table['edges'].to_pylist()

    highly_irregular_by_order = defaultdict(list)

    for idx in range(len(graph6_list)):
        graph6 = graph6_list[idx]
        order = order_list[idx]
        edges = edges_list[idx]

        # Compute neighbor degrees for this graph
        neighbor_degrees = process_graph_edges(edges, order)

        # Check if highly irregular
        if is_highly_irregular(neighbor_degrees):
            highly_irregular_by_order[order].append(graph6)

    return highly_irregular_by_order


def get_optimal_workers():
    """Calculate optimal number of workers for multiprocessing"""
    cpu_physical = psutil.cpu_count(logical=False) or 1
    # Use physical cores - 1 to leave headroom
    optimal_cores = max(1, cpu_physical - 1)
    # 2x parallelism factor
    n_jobs = optimal_cores * 2
    return max(1, n_jobs)


def merge_results(all_results):
    merged = defaultdict(list)
    for result in all_results:
        for order, graphs in result.items():
            merged[order].extend(graphs)
    return merged


def classify_graphs(input_dir, output_dir):
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    # Clear output directory
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    print('=' * 70)
    print(f"Reading graphs from {input_dir}...")

    # Find all parquet files
    parquet_files = list(input_path.rglob("*.parquet"))
    n_jobs = get_optimal_workers()

    print("Processing graphs...")

    # Process files in parallel with optimal chunk size
    if n_jobs > 1 and len(parquet_files) > 1:
        # Use chunksize for better load balancing
        chunksize = max(1, len(parquet_files) // (n_jobs * 4))
        with Pool(n_jobs) as pool:
            results = pool.map(process_parquet_file, parquet_files, chunksize=chunksize)
    else:
        results = [process_parquet_file(f) for f in parquet_files]

    # Merge results efficiently
    results_by_order = merge_results(results)

    total_count = sum(len(graphs) for graphs in results_by_order.values())

    print(f"Found {total_count:,} highly irregular graphs")

    # Write results partitioned by graph_order
    if total_count > 0:
        import pyarrow as pa

        for order, graphs in results_by_order.items():
            order_dir = output_path / f"graph_order={order}"
            order_dir.mkdir(parents=True, exist_ok=True)

            # Efficient PyArrow table creation
            schema = pa.schema([('graph6', pa.string())])
            table = pa.table({'graph6': graphs}, schema=schema)

            output_file = order_dir / "data.parquet"
            pq.write_table(table, str(output_file))

        print(f"Results written to {output_dir}")
    else:
        print("No highly irregular graphs found.")


def main():
    start_time = time.time()

    classify_graphs('input_data', 'simple_output_data')

    end_time = time.time()
    elapsed_time = end_time - start_time

    print("=" * 70)
    print(f"Processing complete! Time taken: {elapsed_time:.2f} seconds")
    print("=" * 70)


if __name__ == "__main__":
    main()