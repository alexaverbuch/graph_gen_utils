graph_gen_utils is a Maven project, built in Eclipse.

It's a helper library designed to quickly create and populate a Neo4j database instance.
Input files must be text files, encoded in the Chaco ﬁle input format [1].

Data is inserted using Neo4j's BatchInserter in 2 passes.
1st pass - read complete Chaco file, insert vertices into Neo4j, and index vertices using Lucene
2nd pass - read complete Chaco file again, insert edges into Neo4j, and index edges using Lucene

Pre-alpha status...
- Using NeoClipse it has been visually tested for correctness when loading graphs of 5 vertices and 10 edges
  But no other testing has been performed yet
- It has had minimal performance tests on Chaco graphs files of approx: 42mb - 450,000 vertices - 3,500,000 edges

To do:
- Improve performance further
- Generate Chaco file from Neo4j instance (currently only supports generating Neo4j from file)
- Add verification functionality to test if generated graphs are correct
- Compare other graph metrics

[1] B. Hendrickson and R. Leland. The Chaco User’s Guide Version 2.0. Tech. Rep. SAND 94-2692, Sandia Natl. Lab.,
Albuquerque, NM, 1994.
