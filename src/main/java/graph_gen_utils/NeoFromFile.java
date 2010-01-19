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

	private static final int NODE_STORE_BUFFER = 100000;
	private static final int REL_STORE_BUFFER = 10000;
	private String inputFile;
	private String storeDir;

	public static void main(String[] args) throws FileNotFoundException {
		NeoFromFile parser = new NeoFromFile("graphs/auto.graph",
				"var/generated-auto");

		long time = System.currentTimeMillis();

		// parser.generateNeo1Pass();
		parser.generateNeo2Pass();

		// PRINTOUTS
		System.out.printf("%n%nTime Taken[%d]: %ds", NeoFromFile.NODE_STORE_BUFFER,
				time / 1000);

	}

	public NeoFromFile(String inputFile, String storeDir) {
		this.inputFile = inputFile;
		this.storeDir = storeDir;
	}

	public void generateNeo1Pass() throws FileNotFoundException {
		File fFile = new File(inputFile);
		Scanner scanner = new Scanner(fFile);
		try {
			// read first line to distinguish format
			if (scanner.hasNextLine()) {
				GraphParser parser = getFormat(scanner.nextLine());

				ArrayList<NodeData> nodes = new ArrayList<NodeData>();

				// read each line to extract node & relationship information
				int nodeNumber = 0;
				while (scanner.hasNextLine()) {
					nodeNumber++;
					nodes.add(parser.parseNodeAndRels(scanner.nextLine(),
							nodeNumber));
				}

				storeNodesAndRelsToNeo(nodes);
				nodes.clear();
			}
		} finally {
			// ensure the underlying stream is always closed
			scanner.close();
		}

	}

	public void generateNeo2Pass() throws FileNotFoundException {

		// PRINTOUTS
		long time = System.currentTimeMillis();
		System.out
				.printf("%nOpening BatchInserter & LuceneIndexBatchInserter Service...");

		BatchInserter batchNeo = new BatchInserterImpl(storeDir,
				BatchInserterImpl.loadProperties("neo.props"));

		LuceneIndexBatchInserter batchIndexService = new LuceneIndexBatchInserterImpl(
				batchNeo);

		// PRINTOUTS
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("%nOpening Graph File...");

		File fFile = new File(inputFile);

		// PRINTOUTS
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("%nReading & Indexing Nodes...");

		GraphParser parser = null;
		Scanner scanner = new Scanner(fFile);
		try {
			// read first line to distinguish format
			if (scanner.hasNextLine()) {
				parser = getFormat(scanner.nextLine());

				ArrayList<NodeData> nodes = new ArrayList<NodeData>();

				// read each line to extract node & relationship information
				int nodeNumber = 0;
				while (scanner.hasNextLine()) {
					nodeNumber++;
					nodes.add(parser.parseNode(scanner.nextLine(), nodeNumber));

					if ((nodeNumber % NeoFromFile.NODE_STORE_BUFFER) == 0) {
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

		// PRINTOUTS
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("%nReading & Indexing Relationships...");

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

					if ((nodeNumber % NeoFromFile.REL_STORE_BUFFER) == 0) {
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

		// PRINTOUTS
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out
				.printf("%nClosing BatchInserter & LuceneIndexBatchInserter Service...");

		batchIndexService.shutdown();
		batchNeo.shutdown();

		// PRINTOUTS
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("%nNeo4j Instance Created!");
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

		switch (format) {
		case 0:
			return new GraphParserUnweighted(nodeCount, edgeCount);
		case 1:
			return new GraphParserWeightedEdges(nodeCount, edgeCount);
		case 10:
			return new GraphParserWeightedNodes(nodeCount, edgeCount);
		case 11:
			return new GraphParserWeighted(nodeCount, edgeCount);
		default:
			return new GraphParserUnweighted(nodeCount, edgeCount);
		}
	}

	private void storeNodesAndRelsToNeo(ArrayList<NodeData> nodesAndRels) {
		BatchInserter batchNeo = new BatchInserterImpl(storeDir,
				BatchInserterImpl.loadProperties("neo.props"));

		LuceneIndexBatchInserter batchIndexService = new LuceneIndexBatchInserterImpl(
				batchNeo);

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

		batchIndexService.shutdown();
		batchNeo.shutdown();
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