# Graph-Analysis
Map-Reduce program that finds the connected components of any undirected graph and prints the size of these connected components.
An undirected graph is represented in the input text file using one line per graph vertex. For example, the line
1,2,3,4,5,6,7 represents the vertex with ID 1, which is connected to the vertices with IDs 2, 3, 4, 5, 6, and 7. 
Task is to write a Map-Reduce program that finds the connected components of any undirected graph and prints the size of these connected components.
A connected component of a graph is a subgraph of the graph in which there is a path from any two vertices in the subgraph. For the small-input-graph, there are two connected components: one 0,8,9 and another 1,2,3,4,5,6,7.
The program should print the sizes of these connected components: 3 and 7.
