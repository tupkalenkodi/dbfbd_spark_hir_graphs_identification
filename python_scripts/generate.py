import subprocess
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import time
import shutil
import os


def decode_graph6(graph6_str: str):
    n = ord(graph6_str[0]) - 63
    s = graph6_str[1:]
    total_bits = n * (n - 1) // 2

    all_bits = []
    for ch in s:
        c = ord(ch) - 63
        all_bits.extend([(c >> (5 - i)) & 1 for i in range(6)])

    bits = all_bits[:total_bits]
    edges = []
    bit_idx = 0

    for j in range(1, n):
        for i in range(j):
            if bit_idx < len(bits):
                if bits[bit_idx] == 1:
                    edges.append([int(i), int(j)])
            else:
                break
            bit_idx += 1
        if bit_idx >= len(bits):
            break

    return edges, n


def process_graphs_batch(graph6_lines):
    batch_data = {
        'graph6': [],
        'graph_order': [],
        'size': [],
        'edges': []
    }

    for g6 in graph6_lines:
        g6 = g6.strip()
        if not g6:
            continue

        edges, n = decode_graph6(g6)

        # Convert edges to string format for Avro compatibility
        edges_str = ";".join([f"{a},{b}" for a, b in edges])

        batch_data['graph6'].append(g6)
        batch_data['graph_order'].append(n)
        batch_data['size'].append(len(edges))
        batch_data['edges'].append(edges_str)

    return batch_data


def write_parquet_direct(batch_data, output_file):
    schema = pa.schema([
        ('graph6', pa.string()),
        ('graph_order', pa.int32()),
        ('size', pa.int32()),
        ('edges', pa.string())
    ])

    table = pa.table(batch_data, schema=schema)
    pq.write_table(table, str(output_file), compression='none')


def generate_graphs(target_number: int, output_dir: Path, batch_size):
    min_n = 1
    max_n = 20

    batch_num = 0
    batch_lines = []
    generated_total_graphs = 0

    for n in range(min_n, max_n + 1):
        if generated_total_graphs >= target_number:
            break
        min_edges = n - 1
        max_edges = (n * (n + 2)) // 8
        max_degree = n // 2

        cmd = [
            "nauty-geng", str(n),
            "-c",
            f"{min_edges}:{max_edges}",
            "-d1", f"-D{max_degree}",
            "-l"
        ]

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, bufsize=1024 * 1024)

        for line in process.stdout:
            line = line.strip()
            if not line:
                continue

            batch_lines.append(line)
            generated_total_graphs += 1

            # Write temporary batch when full
            if len(batch_lines) >= batch_size:
                batch_data = process_graphs_batch(batch_lines)
                output_file = output_dir / f"batch_{batch_num:04d}.parquet"
                write_parquet_direct(batch_data, output_file)

                batch_num += 1
                batch_lines = []

                print(f"  Batch {batch_num} / {target_number // batch_size} graphs generated..")


            if generated_total_graphs >= target_number:
                process.terminate()
                break

        process.wait()

    # Process any remaining graphs
    if batch_lines:
        batch_data = process_graphs_batch(batch_lines, n)

        output_file = output_dir / f"batch_{batch_num:04d}.parquet"
        write_parquet_direct(batch_data, output_file)
        batch_num += 1

    print(f"\nGenerated {generated_total_graphs:,} graphs in {batch_num} batches")


def main():

    output_dir = Path(f"/home/cubic/PROJECTS/db_spark_scala/data/chi/input_data")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    target_number = 500_000_000

    print("=" * 70)
    print(f"GRAPH GENERATION: target number = {target_number}")
    print("=" * 70)

    # Record start time
    start_time = time.time()

    # Run your function
    generate_graphs(target_number, output_dir, batch_size=1_000_000)

    # Record end time
    end_time = time.time()

    # Calculate and print elapsed time
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.2f} seconds")
    print("=" * 70)
    print("Generation complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
    
