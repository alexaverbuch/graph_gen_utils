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
	private String inputFile;
	private String storeDir;

	public static void main(String[] args) throws FileNotFoundException {
		long time;

		NeoFromFile parser1 = new NeoFromFile("graphs/test11.graph",
				"var/generated-test11-1");

		time = System.currentTimeMillis();

		parser1.generateNeo1Pass();

		// PRINTOUT
		System.out.printf("%n--------------------%n");
		System.out.printf("1-Pass (REL_STORE_BUF=%d) - Time Taken: %fs",
				NeoFromFile.REL_STORE_BUF,
				(double) (System.currentTimeMillis() - time) / (double) 1000);
		System.out.printf("%n--------------------%n");

		NeoFromFile neoCreator2 = new NeoFromFile("graphs/test11.graph",
				"var/generated-test11-2");

		time = System.currentTimeMillis();

		neoCreator2.generateNeo2Pass();

		// PRINTOUT
		System.out.printf("%n--------------------%n");
		System.out
				.printf(
						"2-Pass (NODE_STORE_BUF=%d, REL_STORE_BUF=%d) - Time Taken: %fs",
						NeoFromFile.NODE_STORE_BUF, NeoFromFile.REL_STORE_BUF,
						(double) (System.currentTimeMillis() - time)
								/ (double) 1000);
		System.out.printf("%n--------------------%n");
	}

	public NeoFromFile(String inputFile, String storeDir) {
		this.inputFile = inputFile;
		this.storeDir = storeDir;
	}

	public void generateNeo1Pass() throws FileNotFoundException {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out
				.printf("%nOpening BatchInserter & LuceneIndexBatchInserter Service...");

		BatchInserter batchNeo = new BatchInserterImpl(storeDir,
				BatchInserterImpl.loadProperties("neo.props"));

		LuceneIndexBatchInserter batchIndexService = new LuceneIndexBatchInserterImpl(
				batchNeo);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("Opening Graph File...");

		File fFile = new File(inputFile);

		Scanner scanner = new Scanner(fFile);
		try {
			// read first line to distinguish format
			if (scanner.hasNextLine()) {
				// PRINTOUT
				System.out.printf("%dms%n", System.currentTimeMillis() - time);

				GraphParser parser = getFormat(scanner.nextLine());

				// PRINTOUT
				time = System.currentTimeMillis();
				System.out
						.printf("Reading & Indexing Nodes & Relationships...");

				ArrayList<NodeData> nodes = new ArrayList<NodeData>();

				// read each line to extract node & relationship information
				int nodeNumber = 0;
				while (scanner.hasNextLine()) {
					nodeNumber++;
					nodes.add(parser.parseNodeAndRels(scanner.nextLine(),
							nodeNumber));

					if ((nodeNumber % NeoFromFile.REL_STORE_BUF) == 0) {

						// PRINTOUT
						System.out.printf(".");
					}
				}

				storeNodesAndRelsToNeo(nodes, batchNeo, batchIndexService);
				nodes.clear();
			}
		} finally {
			// ensure the underlying stream is always closed
			scanner.close();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out
				.printf("Closing BatchInserter & LuceneIndexBatchInserter Service...");

		batchIndexService.shutdown();
		batchNeo.shutdown();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("Neo4j Instance Created!");
	}

	public void generateNeo2Pass() throws FileNotFoundException {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out
				.printf("Opening BatchInserter & LuceneIndexBatchInserter Service...");

		BatchInserter batchNeo = new BatchInserterImpl(storeDir,
				BatchInserterImpl.loadProperties("neo.props"));

		LuceneIndexBatchInserter batchIndexService = new LuceneIndexBatchInserterImpl(
				batchNeo);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("Opening Graph File...");

		File fFile = new File(inputFile);

		GraphParser parser = null;
		Scanner scanner = new Scanner(fFile);
		try {
			// read first line to distinguish format
			if (scanner.hasNextLine()) {
				// PRINTOUT
				System.out.printf("%dms%n", System.currentTimeMillis() - time);

				parser = getFormat(scanner.nextLine());

				// PRINTOUT
				time = System.currentTimeMillis();
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

						storeNodesToNeo(nodes, batchNeo, batchIndexService);
						nodes.clear();
					}
				}

				storeNodesToNeo(nodes, batchNeo, batchIndexService);
				nodes.clear();

				batchIndexService.optimize();
			}
		} finally {
			// ensure the underlying stream is always closed
			scanner.close();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("Reading & Indexing Relationships...");

		scanner = new Scanner(fFile);
		try {
			// read first line to distinguish format
			if (scanner.hasNextLine()) {

				// skip over first line
				scanner.nextLine();

				ArrayList<NodeData> nodesAndRels = new ArrayList<NodeData>();

				// read each line to extract node & relationship information
				int nodeNumber = 0;
				while (scanner.hasNextLine()) {

					nodeNumber++;
					nodesAndRels.add(parser.parseNodeAndRels(
							scanner.nextLine(), nodeNumber));

					if ((nodeNumber % NeoFromFile.REL_STORE_BUF) == 0) {
						System.out.printf(".");
						storeNodesToNeo(nodesAndRels, batchNeo,
								batchIndexService);
						nodesAndRels.clear();
					}
				}

				storeRelsToNeo(nodesAndRels, batchNeo, batchIndexService);
				nodesAndRels.clear();
			}
		} finally {
			// ensure the underlying stream is always closed
			scanner.close();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out
				.printf("Closing BatchInserter & LuceneIndexBatchInserter Service...");

		batchIndexService.shutdown();
		batchNeo.shutdown();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("Neo4j Instance Created!");
	}

	private GraphParser getFormat(String aLine) {
		StringTokenizer st = new StringTokenizer(aLine, " ");

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

		System.out.printf("\tnodes \t= %d%n", nodeCount);
		System.out.printf("\tedges \t= %d%n", edgeCount);

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

	private void storeNodesAndRelsToNeo(ArrayList<NodeData> nodesAndRels,
			BatchInserter batchNeo, LuceneIndexBatchInserter batchIndexService) {

		for (NodeData nodeAndRels : nodesAndRels) {
			long nodeID = batchNeo.createNode(nodeAndRels.getProperties());

			batchIndexService.index(nodeID, "name", nodeAndRels.getProperties()
					.get("name"));
			batchIndexService.index(nodeID, "weight", nodeAndRels
					.getProperties().get("weight"));
		}

		batchIndexService.optimize();

		for (NodeData nodeAndRels : nodesAndRels) {
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

	private void storeNodesToNeo(ArrayList<NodeData> nodes,
			BatchInserter batchNeo, LuceneIndexBatchInserter batchIndexService) {

		for (NodeData nodeAndRels : nodes) {
			long nodeID = batchNeo.createNode(nodeAndRels.getProperties());

			batchIndexService.index(nodeID, "name", nodeAndRels.getProperties()
					.get("name"));
			batchIndexService.index(nodeID, "weight", nodeAndRels
					.getProperties().get("weight"));
		}

		batchIndexService.optimize();
	}

	private void storeRelsToNeo(ArrayList<NodeData> nodes,
			BatchInserter batchNeo, LuceneIndexBatchInserter batchIndexService) {

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