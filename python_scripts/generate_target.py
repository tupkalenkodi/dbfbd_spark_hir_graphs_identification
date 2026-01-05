import networkx as nx
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import numpy as np
import random
import shutil
import matplotlib.pyplot as plt


def visualize_graph(graph, output_dir):
    plt.figure(figsize=(12, 10))

    # Choose layout
    # For small graphs (< 100 nodes), spring layout works well
    if graph.number_of_nodes() < 100:
        pos = nx.spring_layout(graph, k=0.5, iterations=50, seed=42)
    else:
        # For larger graphs, use kamada_kawai
        pos = nx.kamada_kawai_layout(graph)

    # Draw the graph
    nx.draw_networkx_nodes(graph, pos,
                           node_color='lightblue',
                           node_size=500,
                           alpha=0.9)

    nx.draw_networkx_edges(graph, pos,
                           width=1.5,
                           alpha=0.6,
                           edge_color='gray')

    nx.draw_networkx_labels(graph, pos,
                            font_size=10,
                            font_weight='bold')

    plt.title(f"Graph with {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges")
    plt.axis('off')
    plt.tight_layout()

    # Optionally display
    plt.show()

def setup_output_directory(output_dir):
    if output_dir.exists():
        shutil.rmtree(output_dir)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    return output_dir


def generate_graph(num_components, component_sizes, graph_type, **kwargs):

    sizes = []
    (m, s) = component_sizes
    # Generate sizes from normal distribution
    for _ in range(num_components):
        size = np.random.normal(m, s)

        # Round to nearest integer and apply bounds
        size = round(size)
        sizes.append(size)

    # Ensure all sizes are integers
    sizes = [int(size) for size in sizes]

    components = []
    node_offset = 0

    print('\n')
    print('*' * 70)

    for i, size in enumerate(sizes):
        print(f"Generating component {i+1}/{num_components} with {size} nodes...")

        # Generate component based on type
        if graph_type == "erdos_renyi":
            p = kwargs.get('p', 0.3)
            graph = nx.erdos_renyi_graph(size, p)
        elif graph_type == "barabasi_albert":
            m = kwargs.get('m', 3)
            graph = nx.barabasi_albert_graph(size, m)
        elif graph_type == "watts_strogatz":
            k = kwargs.get('k', 4)
            p = kwargs.get('p', 0.3)
            graph = nx.watts_strogatz_graph(size, k, p)
        elif graph_type == "random_regular":
            d = kwargs.get('d', 3)
            graph = nx.random_regular_graph(d, size)
        else:
            return None

        # Relabel nodes to avoid conflicts
        mapping = {old: f"{node_offset + old}" for old in graph.nodes()}
        graph = nx.relabel_nodes(graph, mapping)

        components.append(graph)
        node_offset += size

    # Combine all components into one graph
    combined_graph = nx.Graph()
    for component in components:
        combined_graph = nx.compose(combined_graph, component)

    return combined_graph


def print_statistics(graph):
    degrees = [d for n, d in graph.degree()]

    print('\n')
    print("GRAPH STATISTICS")
    print("-" * 70)
    print(f"Nodes: {graph.number_of_nodes()}")
    print(f"Edges: {graph.number_of_edges()}")
    print(f"Connected components: {nx.number_connected_components(graph)}")
    print(f"Average degree: {sum(degrees) / len(degrees):.2f}")
    print(f"Min degree: {min(degrees)}")
    print(f"Max degree: {max(degrees)}")

    print("*" * 70)
    print('\n')


def save_to_parquet(graph, output_dir, vertices_path=None, edges_path=None):
    if vertices_path is None:
        vertices_path = output_dir / "vertices.parquet"
    if edges_path is None:
        edges_path = output_dir / "edges.parquet"

    # Create vertices data
    vertices_data = {
        'id': [node for node in graph.nodes()]
    }

    # Create edges data
    edges_data = {
        'src': [],
        'dst': []
    }

    for u, v in graph.edges():
        edges_data['src'].append(u)
        edges_data['dst'].append(v)

    # Write vertices
    vertices_schema = pa.schema([
        ('id', pa.string())
    ])
    vertices_table = pa.table(vertices_data, schema=vertices_schema)
    pq.write_table(vertices_table, str(vertices_path), compression='none')

    # Write edges
    edges_schema = pa.schema([
        ('src', pa.string()),
        ('dst', pa.string())
    ])
    edges_table = pa.table(edges_data, schema=edges_schema)
    pq.write_table(edges_table, str(edges_path), compression='none')


def main():
    # Get the script's directory (where this Python file is located)
    script_dir = Path(__file__).parent

    # Directory at the project root:
    project_root = script_dir.parent
    output_dir = project_root / "data" / "target" / "large"

    # Setup output directory
    output_dir = setup_output_directory(output_dir)

    # Multi-component options
    num_components = 5
    # Set to [20, 30, 40] for specific sizes, or None for random
    component_sizes_mean_sd = (100, 10)

    # Graph type: 'erdos_renyi', 'barabasi_albert', 'watts_strogatz',
    #             'random_regular', 'powerlaw_cluster', 'complete', 'cycle'
    graph_type = 'barabasi_albert'

    # Graph parameters
    p = 0.3   # Probability parameter (for erdos_renyi, watts_strogatz)
    m = 3     # Edge parameter (for barabasi_albert, powerlaw_cluster)
    k = 6     # Degree parameter (for watts_strogatz)
    d = 4     # Degree for random_regular graphs

    # Random seed for reproducibility
    seed = 42
    random.seed(seed)

    print("=" * 70)
    print(f"TARGET GRAPH GENERATION")
    print("=" * 70)

    # Generate graph
    graph = generate_graph(num_components=num_components,component_sizes=component_sizes_mean_sd,
                           graph_type=graph_type, p=p, m=m, k=k, d=d)

    # Print statistics
    print("Printing Statistics...")
    print_statistics(graph)

    # Save to Parquet
    print("Saving Files...")
    save_to_parquet(graph, output_dir)

    # Add visualization
    # visualize_graph(graph, output_dir)

    print("=" * 70)
    print("GENERATION COMPLETE!")
    print("=" * 70)

if __name__ == "__main__":
    main()
