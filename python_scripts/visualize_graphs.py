import networkx as nx
import matplotlib.pyplot as plt


def visualize_graph6(graph6_string):
    # CREATE GRAPH FROM graph6
    g = nx.from_graph6_bytes(graph6_string.encode())

    # BASIC VISUALIZATION
    plt.figure(figsize=(6, 6))
    nx.draw(g,
            with_labels=True,
            node_color='lightgreen',
            edge_color='gray',
            node_size=300,
            font_size=10)

    plt.show()

if __name__ == "__main__":
    graph6 = "I??Q?Sh`g"

    visualize_graph6(graph6)