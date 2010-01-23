package graph_gen_utils;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.index.lucene.LuceneIndexBatchInserter;
import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class NeoFromFile {

	private static final int NODE_STORE_BUF = 100000;
	private static final int REL_STORE_BUF = 10000;
	private String sourceFile;
	BatchInserter batchNeo = null;
	LuceneIndexBatchInserter batchIndexService = null;

	public static void main(String[] args) throws FileNotFoundException {

		long time;

		NeoFromFile neoCreator = new NeoFromFile("graphs/auto.graph",
				"var/generated-auto");

		time = System.currentTimeMillis();

		neoCreator.generateNeo();

		// PRINTOUT
		System.out.printf("--------------------%n");
		System.out.printf("Neo Created - Time Taken: %fs", (double) (System
				.currentTimeMillis() - time)
				/ (double) 1000);
		System.out.printf("%n--------------------%n");
	}

	public NeoFromFile(String sourceFile, String destinationDir) {
		this.sourceFile = sourceFile;

		System.out.printf("NeoFromFile Settings:%n");
		System.out.printf("\tNODE_STORE_BUF\t= %d%n", NeoFromFile.NODE_STORE_BUF);
		System.out.printf("\tREL_STORE_BUF\t= %d%n", NeoFromFile.REL_STORE_BUF);

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out
				.printf("Opening BatchInserter & LuceneIndexBatchInserter...");

		batchNeo = new BatchInserterImpl(destinationDir, BatchInserterImpl
				.loadProperties("neo.props"));

		batchIndexService = new LuceneIndexBatchInserterImpl(batchNeo);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	public void generateNeoPartitioned(String sourcePartitioning)
			throws FileNotFoundException {

		generateNeo();

	}

	public void generateNeo() throws FileNotFoundException {

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Graph File...");

		File graphFile = new File(sourceFile);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		GraphParser parser = getParser(graphFile);

		storeNodesToNeo(graphFile, parser);

		storeRelsToNeo(graphFile, parser);

		time = System.currentTimeMillis();

		// PRINTOUT
		System.out
				.printf("Closing BatchInserter & LuceneIndexBatchInserter...");

		batchIndexService.shutdown();
		batchNeo.shutdown();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private GraphParser getParser(File graphFile) throws FileNotFoundException {

		Scanner scanner = new Scanner(graphFile);

		// read first line to distinguish format
		StringTokenizer st = new StringTokenizer(scanner.nextLine(), " ");

		scanner.close();

		int nodeCount = 0;
		int edgeCount = 0;
		int format = 0;

		if (st.hasMoreTokens()) {
			nodeCount = Integer.parseInt(st.nextToken());
		}

		if (st.hasMoreTokens()) {
			edgeCount = Integer.parseInt(st.nextToken());
		}

		if (st.hasMoreTokens()) {
			format = Integer.parseInt(st.nextToken());
		}

		System.out.printf("Graph Properties:%n");
		System.out.printf("\tNodes \t= %d%n", nodeCount);
		System.out.printf("\tEdges \t= %d%n", edgeCount);

		switch (format) {
		case 0:
			System.out.printf("\tFormat \t= Unweighted%n");
			return new GraphParserUnweighted(nodeCount, edgeCount);
		case 1:
			System.out.printf("\tFormat \t= Weighted Edges%n");
			return new GraphParserWeightedEdges(nodeCount, edgeCount);
		case 10:
			System.out.printf("\tFormat \t= Weighted Nodes%n");
			return new GraphParserWeightedNodes(nodeCount, edgeCount);
		case 11:
			System.out.printf("\tFormat \t= Weighted%n");
			return new GraphParserWeighted(nodeCount, edgeCount);
		default:
			System.out.printf("\tFormat \t= Unweighted%n");
			return new GraphParserUnweighted(nodeCount, edgeCount);
		}
	}

	private void storeNodesToNeo(File graphFile, GraphParser parser)
			throws FileNotFoundException {

		Scanner scanner = new Scanner(graphFile);

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Reading & Indexing Nodes...");

		ArrayList<NodeData> nodes = new ArrayList<NodeData>();

		// read each line to extract node & relationship information
		int nodeNumber = 0;
		while (scanner.hasNextLine()) {
			nodeNumber++;
			nodes.add(parser.parseNode(scanner.nextLine(), nodeNumber));

			if ((nodeNumber % NeoFromFile.NODE_STORE_BUF) == 0) {

				// PRINTOUT
				System.out.printf(".");

				flushNodes(nodes);
				nodes.clear();
			}
		}

		flushNodes(nodes);
		nodes.clear();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("Optimizing Index...");

		batchIndexService.optimize();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		scanner.close();

	}

	private void storeRelsToNeo(File graphFile, GraphParser parser)
			throws FileNotFoundException {

		// PRINTOUT
		System.out.printf("Reading & Indexing Relationships...");

		long time = System.currentTimeMillis();
		Scanner scanner = new Scanner(graphFile);

		// skip over first line
		scanner.nextLine();

		ArrayList<NodeData> nodesAndRels = new ArrayList<NodeData>();

		// read each line to extract node & relationship information
		int nodeNumber = 0;
		while (scanner.hasNextLine()) {

			nodeNumber++;
			nodesAndRels.add(parser.parseNodeAndRels(scanner.nextLine(),
					nodeNumber));

			if ((nodeNumber % NeoFromFile.REL_STORE_BUF) == 0) {
				System.out.printf(".");
				flushNodes(nodesAndRels);
				nodesAndRels.clear();
			}
		}

		flushRels(nodesAndRels);
		nodesAndRels.clear();

		scanner.close();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

	}

	private void flushNodes(ArrayList<NodeData> nodes) {

		for (NodeData nodeAndRels : nodes) {
			long nodeID = batchNeo.createNode(nodeAndRels.getProperties());

			batchIndexService.index(nodeID, "name", nodeAndRels.getProperties()
					.get("name"));
			batchIndexService.index(nodeID, "weight", nodeAndRels
					.getProperties().get("weight"));
		}

		batchIndexService.optimize();
	}

	private void flushRels(ArrayList<NodeData> nodes) {

		for (NodeData nodeAndRels : nodes) {
			long fromNodeID = batchIndexService.getNodes("name",
					nodeAndRels.getProperties().get("name")).iterator().next();

			for (Map<String, Object> rel : nodeAndRels.getRelationships()) {
				String name = (String) rel.get("name");

				long toNodeID = batchIndexService.getNodes("name", name)
						.iterator().next();

				rel.put("name", nodeAndRels.getProperties().get("name") + "->"
						+ name);

				batchNeo.createRelationship(fromNodeID, toNodeID,
						DynamicRelationshipType.withName("KNOWS"), rel);
			}
		}
	}
}