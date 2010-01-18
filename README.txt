Helper library to quickly create and populate a Neo4j database instance.
Input files must be text files, encoded in the Chaco ﬁle input format [1].
Data is inserted using Neo4j's BatchInserter.

Pre-alpha status...
- Using NeoClipse it has been visually tested for correctness when loading graphs of 5 vertices and 10 edges
- Has had minimal performance tests on graphs with approx 30,000 vertices and 1,000,000 edges

To do:
- Improve performance
- Generate Chaco file from Neo4j instance (currently only supports generating Neo4j from file)
- Add verification functionality to test if generated graphs are correct

[1] B. Hendrickson and R. Leland. The Chaco User’s Guide Version 2.0. Tech. Rep. SAND 94-2692, Sandia Natl. Lab.,
Albuquerque, NM, 1994.
