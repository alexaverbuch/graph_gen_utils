// Author: Alex Averbuch & Martin Neumann

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

	private static final int STORE_BUFFER = 100;
	private String inputFile;
	private String storeDir;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws FileNotFoundException {
		NeoFromFile parser = new NeoFromFile("graphs/test0.graph",
				"var/generated-test0");

		System.out.printf("Generating Neo... ");

		parser.generateNeo();

		System.out.printf("Complete");
	}

	public NeoFromFile(String inputFile, String storeDir) {
		this.inputFile = inputFile;
		this.storeDir = storeDir;
	}

	public void generateNeo() throws FileNotFoundException {
		BatchInserter batchNeo = new BatchInserterImpl(storeDir,
				BatchInserterImpl.loadProperties("neo.props"));

		LuceneIndexBatchInserter batchIndexService = new LuceneIndexBatchInserterImpl(
				batchNeo);

		File fFile = new File(inputFile);
		Scanner scanner = new Scanner(fFile);
		try {
			// read first line to distinguish format
			if (scanner.hasNextLine()) {
				GraphParser parser = getFormat(scanner.nextLine());

				ArrayList<NodeData> nodes = new ArrayList<NodeData>();

				// read each line to extract node & relationship information
				int nodeCount = 0;
				while (scanner.hasNextLine()) {
					nodeCount++;
					nodes.add(parser.parseLine(scanner.nextLine(), nodeCount));

					// Commented out because at this point some relationships
					// may reference unknown nodes.
					// Assuming we have enough RAM to reduce complexity for the
					// time being.
					// if ((nodeCount % GraphFromFile.STORE_BUFFER) == 0) {
					// storeToNeo(nodes, batchNeo, batchIndexService);
					// nodes.clear();
					// }
				}

				storeToNeo(nodes, batchNeo, batchIndexService);
				nodes.clear();
			}
		} finally {
			// ensure the underlying stream is always closed
			scanner.close();
			batchIndexService.shutdown();
			batchNeo.shutdown();
		}

	}

	private GraphParser getFormat(String aLine) {
		StringTokenizer st = new StringTokenizer(aLine," ");
//		while (st.hasMoreTokens()) {
//			st.nextToken();
//		}
		
//		Scanner scanner = new Scanner(aLine);
//		scanner.useDelimiter(" ");

		int nodeCount = 0;
		int edgeCount = 0;
		int format = 0;

		if (st.hasMoreTokens()) {
//		if (scanner.hasNextLine()) {
			nodeCount = Integer.parseInt(st.nextToken());
		}

		if (st.hasMoreTokens()) {
//		if (scanner.hasNextLine()) {
			edgeCount = Integer.parseInt(st.nextToken());
		}

		if (st.hasMoreTokens()) {
//		if (scanner.hasNextLine()) {
			format = Integer.parseInt(st.nextToken());
		}

//		scanner.close();

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

	private void storeToNeo(ArrayList<NodeData> nodes, BatchInserter batchNeo,
			LuceneIndexBatchInserter batchIndexService) {

		for (NodeData node : nodes) {
			long nodeID = batchNeo.createNode(node.getProperties());

			batchIndexService.index(nodeID, "name", node.getProperties().get(
					"name"));
			batchIndexService.index(nodeID, "weight", node.getProperties().get(
					"weight"));
		}

		batchIndexService.optimize();

		for (NodeData node : nodes) {
			long fromNodeID = batchIndexService.getNodes("name",
					node.getProperties().get("name")).iterator().next();

			for (Map<String, Object> relData : node.getRelationships()) {
				String name = (String) relData.get("name");

				long toNodeID = batchIndexService.getNodes("name", name)
						.iterator().next();

				relData.put("name", node.getProperties().get("name") + "->"
						+ name);

				batchNeo.createRelationship(fromNodeID, toNodeID,
						DynamicRelationshipType.withName("KNOWS"), relData);
			}
		}
	}
}
