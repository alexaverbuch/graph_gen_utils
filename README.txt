graph_gen_utils is a Maven project, built in Eclipse.

Quickly import/export Neo4j database instances from/to different formats, including:
	* Chaco files (+ Partition files) [1]
	* GML files
	* Generated topologeies (Random & Fully Connected)
	* Proprietry format to import very big datasets from Twitter crawls

Apply "partitionings" (a COLOR property is added to Nodes that represents the partition it belongs to) to a Neo4j instance:
	* PartitionerAsRandom
	* PartitionerAsBalanced (round robin partition allocation)
	* PartitionerAsFile (Partition files) [1]
	* PartitionerAsCoords (useful for GIS datasets)
	* Partitioner... custom schemes possible by extending Partitioner

Load Neo4j database instance into memory:
	* MemGraph (an in-memory implementation of the Neo4j GraphDatabaseService interface)

Write graph metrics to .csv file:
	Graph metrics (general):
		* Clustering-coefficient
		* Number of vertices/nodes
		* Number of edges/relationships

	Graph metrics (partitioning related):
		* Modularity
		* Edge cut
		* Partition sizes

Neo4j database import procedure is memory optimized, and performance optimized via option to use batch inserter

[1] B. Hendrickson and R. Leland. The Chaco User’s Guide Version 2.0. Tech. Rep. SAND 94-2692, Sandia Natl. Lab.,
Albuquerque, NM, 1994.
