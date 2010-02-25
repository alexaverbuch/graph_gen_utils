package graph_gen_utils;

import graph_gen_utils.chaco.ChacoParser;
import graph_gen_utils.chaco.ChacoParserUnweighted;
import graph_gen_utils.chaco.ChacoParserWeighted;
import graph_gen_utils.chaco.ChacoParserWeightedEdges;
import graph_gen_utils.chaco.ChacoParserWeightedNodes;
import graph_gen_utils.chaco.ChacoWriter;
import graph_gen_utils.chaco.ChacoWriterUnweighted;
import graph_gen_utils.general.NodeData;
import graph_gen_utils.metrics.MetricsWriterUnweighted;
import graph_gen_utils.topology.GraphTopology;
import graph_gen_utils.topology.GraphTopologyFullyConnected;
import graph_gen_utils.topology.GraphTopologyRandom;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexBatchInserter;
import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class NeoFromFile {

	public enum ChacoType {
		UNWEIGHTED, WEIGHTED_EDGES, WEIGHTED_NODES, WEIGHTED
	}

	public enum ClusterInitType {
		RANDOM, BALANCED, SINGLE
	}

	private static final int NODE_STORE_BUF = 100000;
	private static final int REL_STORE_BUF = 10000;

	private String databaseDir;

	private BatchInserter batchNeo = null;
	private LuceneIndexBatchInserter batchIndexService = null;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	public static void main(String[] args) throws Exception {

		long time = System.currentTimeMillis();

		// NeoFromFile neoCreator = new NeoFromFile("var/test11");
		// neoCreator.writeNeo("graphs/test11.graph");

		// NeoFromFile neoCreator = new NeoFromFile("var/test-connected");
		// neoCreator.writeNeo(new GraphTopologyFullyConnected(5));

		NeoFromFile neoCreator = new NeoFromFile("var/test-connected");

		// neoCreator.writeNeo("graphs/random-1000-5.graph",
		// ClusterInitType.BALANCED, 16);
		// neoCreator.writeNeo(new GraphTopologyFullyConnected(5),
		// ClusterInitType.RANDOM, 2);
		neoCreator.writeNeo(new GraphTopologyFullyConnected(5));
		// neoCreator.writeChacoAndPtn("temp/connected-1000-5.graph",
		// ChacoType.UNWEIGHTED, "temp/random-1000-5-BAL.16.ptn");
		neoCreator.writeChaco("temp/connected-5.graph", ChacoType.UNWEIGHTED);

		// PRINTOUT
		System.out.printf("--------------------%n");
		System.out.printf("Finished - Time Taken: %fs", (double) (System
				.currentTimeMillis() - time)
				/ (double) 1000);
		System.out.printf("%n--------------------%n");

	}

	public NeoFromFile(String databaseDir) {
		this.databaseDir = databaseDir;

		System.out.printf("NeoFromFile Settings:%n");
		System.out.printf("\tNODE_STORE_BUF\t= %d%n",
				NeoFromFile.NODE_STORE_BUF);
		System.out.printf("\tREL_STORE_BUF\t= %d%n", NeoFromFile.REL_STORE_BUF);
	}

	// PUBLIC METHODS

	public void writeNeo(String graphPath) throws FileNotFoundException {

		openBatchServices();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Graph File...");

		File graphFile = new File(graphPath);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		ChacoParser parser = getParser(graphFile);

		storeNodesToNeo(graphFile, parser);

		storeRelsToNeo(graphFile, parser);

		closeBatchServices();

	}

	public void writeNeo(GraphTopology topology) throws Exception {

		storePartitionedNodesAndRelsToNeo(topology, ClusterInitType.SINGLE, -1);

	}

	public void writeNeo(GraphTopology topology,
			ClusterInitType clusterInitType, int ptnVal) throws Exception {

		storePartitionedNodesAndRelsToNeo(topology, clusterInitType, ptnVal);

	}

	public void writeNeo(String graphPath, ClusterInitType clusterInitType,
			int ptnVal) throws Exception {

		openBatchServices();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Graph File...");

		File graphFile = new File(graphPath);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		ChacoParser parser = getParser(graphFile);

		storePartitionedNodesToNeo(graphFile, clusterInitType, ptnVal, parser);

		closeBatchServices();

		openTransServices();

		storePartitionedRelsToNeo(graphFile, parser);

		closeTransServices();

	}

	public void writeNeo(String graphPath, String partitionPath)
			throws FileNotFoundException {

		openBatchServices();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Graph & Partitioning Files...");

		File graphFile = new File(graphPath);
		File partitionFile = new File(partitionPath);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		ChacoParser parser = getParser(graphFile);

		storePartitionedNodesToNeo(graphFile, partitionFile, parser);

		closeBatchServices();

		openTransServices();

		storePartitionedRelsToNeo(graphFile, parser);

		closeTransServices();

	}

	public void writeChaco(String chacoPath, ChacoType chacoType) {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Chaco File...");

		File chacoFile = null;
		ChacoWriter chacoWriter = getWriter(chacoType);

		chacoFile = new File(chacoPath);

		chacoWriter.write(transNeo, chacoFile);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();

	}

	public void writeChacoAndPtn(String chacoPath, ChacoType chacoType,
			String ptnPath) {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Chaco & Partition Files...");

		File chacoFile = null;
		File ptnFile = null;

		ChacoWriter chacoWriter = getWriter(chacoType);

		chacoFile = new File(chacoPath);
		ptnFile = new File(ptnPath);

		chacoWriter.write(transNeo, chacoFile, ptnFile);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();
	}

	public void writeMetrics(String metricsPath) {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Metrics File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUnweighted.writeMetrics(transNeo, metricsFile);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();

	}

	public void writeMetricsCSV(String metricsPath) {
		writeMetricsCSV(metricsPath, null);
	}

	public void writeMetricsCSV(String metricsPath, Long timeStep) {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Metrics CSV File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUnweighted
				.writeMetricsCSV(transNeo, metricsFile, timeStep);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();

	}

	public void appendMetricsCSV(String metricsPath) {
		appendMetricsCSV(metricsPath, null);
	}

	public void appendMetricsCSV(String metricsPath, Long timeStep) {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Appending Metrics CSV File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUnweighted.appendMetricsCSV(transNeo, metricsFile,
				timeStep);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();

	}

	// PRIVATE METHODS

	private ChacoWriter getWriter(ChacoType chacoType) {
		switch (chacoType) {
		case UNWEIGHTED:
			return new ChacoWriterUnweighted();
		case WEIGHTED_EDGES:
			return null;
		case WEIGHTED_NODES:
			return null;
		case WEIGHTED:
			return null;
		default:
			return null;
		}
	}

	private ChacoParser getParser(File graphFile) throws FileNotFoundException {

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
			return new ChacoParserUnweighted(nodeCount, edgeCount);
		case 1:
			System.out.printf("\tFormat \t= Weighted Edges%n");
			return new ChacoParserWeightedEdges(nodeCount, edgeCount);
		case 10:
			System.out.printf("\tFormat \t= Weighted Nodes%n");
			return new ChacoParserWeightedNodes(nodeCount, edgeCount);
		case 11:
			System.out.printf("\tFormat \t= Weighted%n");
			return new ChacoParserWeighted(nodeCount, edgeCount);
		default:
			System.out.printf("\tFormat \t= Unweighted%n");
			return new ChacoParserUnweighted(nodeCount, edgeCount);
		}
	}

	private void storeNodesToNeo(File graphFile, ChacoParser parser)
			throws FileNotFoundException {

		Scanner scanner = new Scanner(graphFile);

		// skip first line (file format)
		scanner.nextLine();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Reading & Indexing Nodes...");

		ArrayList<NodeData> nodes = new ArrayList<NodeData>();

		// read each line to extract node & relationship information
		int nodeNumber = 0;
		while (scanner.hasNextLine()) {
			nodeNumber++;
			NodeData node = parser.parseNode(scanner.nextLine(), nodeNumber);
			node.getProperties().put("color", -1);
			nodes.add(node);

			if ((nodeNumber % NeoFromFile.NODE_STORE_BUF) == 0) {

				// PRINTOUT
				System.out.printf(".");

				flushNodesBatch(nodes);
				nodes.clear();
			}
		}

		flushNodesBatch(nodes);
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

	private void storePartitionedNodesAndRelsToNeo(GraphTopology topology,
			ClusterInitType clusterInitType, int ptnVal) throws Exception {

		openBatchServices();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Reading & Indexing Nodes...");

		ArrayList<NodeData> nodesAndRels = topology.getNodesAndRels();

		switch (clusterInitType) {
		case RANDOM:
			initPtnAsRandom(nodesAndRels, ptnVal);
			break;
		case BALANCED:
			initPtnAsBalanced(nodesAndRels, -1, ptnVal);
			break;
		case SINGLE:
			initPtnAsSingle(nodesAndRels, ptnVal);
			break;
		default:
			System.err.println("ClusterInitType not supported");
			throw new Exception("ClusterInitType not supported");
		}

		flushNodesBatch(nodesAndRels);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("Optimizing Index...");

		batchIndexService.optimize();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeBatchServices();

		openTransServices();

		// PRINTOUT
		System.out.printf("Reading & Indexing Relationships...");

		time = System.currentTimeMillis();

		flushRelsTrans(nodesAndRels);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();
	}

	private void storePartitionedNodesToNeo(File graphFile, File partitionFile,
			ChacoParser parser) throws FileNotFoundException {

		Scanner graphScanner = new Scanner(graphFile);
		Scanner partitionScanner = new Scanner(partitionFile);

		long time = System.currentTimeMillis();

		// skip first line (file format)
		graphScanner.nextLine();

		// PRINTOUT
		System.out.printf("Reading & Indexing Nodes...");

		ArrayList<NodeData> nodes = new ArrayList<NodeData>();

		// read each line to extract node & relationship information
		int nodeNumber = 0;
		while (graphScanner.hasNextLine()) {
			nodeNumber++;
			nodes.add(parser.parseNode(graphScanner.nextLine(), nodeNumber));

			if ((nodeNumber % NeoFromFile.NODE_STORE_BUF) == 0) {

				initPtnAsFile(partitionScanner, nodes);

				// PRINTOUT
				System.out.printf(".");

				flushNodesBatch(nodes);
				nodes.clear();
			}
		}

		initPtnAsFile(partitionScanner, nodes);

		flushNodesBatch(nodes);
		nodes.clear();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("Optimizing Index...");

		batchIndexService.optimize();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		graphScanner.close();
	}

	private void storePartitionedNodesToNeo(File graphFile,
			ClusterInitType clusterInitType, int ptnVal, ChacoParser parser)
			throws Exception {

		Scanner graphScanner = new Scanner(graphFile);

		long time = System.currentTimeMillis();

		// skip first line (file format)
		graphScanner.nextLine();

		// PRINTOUT
		System.out.printf("Reading & Indexing Nodes...");

		ArrayList<NodeData> nodes = new ArrayList<NodeData>();

		// read each line to extract node & relationship information
		int nodeNumber = 0;
		int lastPtn = -1;
		while (graphScanner.hasNextLine()) {
			nodeNumber++;
			nodes.add(parser.parseNode(graphScanner.nextLine(), nodeNumber));

			if ((nodeNumber % NeoFromFile.NODE_STORE_BUF) == 0) {

				switch (clusterInitType) {
				case RANDOM:
					initPtnAsRandom(nodes, ptnVal);
					break;
				case BALANCED:
					lastPtn = initPtnAsBalanced(nodes, lastPtn, ptnVal);
					break;
				case SINGLE:
					initPtnAsSingle(nodes, ptnVal);
					break;
				default:
					System.err.println("ClusterInitType not supported");
					throw new Exception("ClusterInitType not supported");
				}

				// PRINTOUT
				System.out.printf(".");

				flushNodesBatch(nodes);
				nodes.clear();
			}
		}

		switch (clusterInitType) {
		case RANDOM:
			initPtnAsRandom(nodes, ptnVal);
			break;
		case BALANCED:
			initPtnAsBalanced(nodes, lastPtn, ptnVal);
			break;
		case SINGLE:
			initPtnAsSingle(nodes, ptnVal);
			break;
		default:
			System.err.println("ClusterInitType not supported");
			throw new Exception("ClusterInitType not supported");
		}

		flushNodesBatch(nodes);
		nodes.clear();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
		time = System.currentTimeMillis();
		System.out.printf("Optimizing Index...");

		batchIndexService.optimize();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		graphScanner.close();
	}

	private void initPtnAsFile(Scanner partitionScanner,
			ArrayList<NodeData> nodes) {

		for (NodeData tempNode : nodes) {
			Integer color = Integer.parseInt(partitionScanner.nextLine());
			tempNode.getProperties().put("color", color);
		}

	}

	private void initPtnAsRandom(ArrayList<NodeData> nodes, int maxPtn) {

		Random rand = new Random(System.currentTimeMillis());

		for (NodeData tempNode : nodes) {
			Integer color = rand.nextInt(maxPtn);
			tempNode.getProperties().put("color", color);
		}

	}

	private int initPtnAsBalanced(ArrayList<NodeData> nodes, int lastPtn,
			int maxPtn) {

		for (NodeData tempNode : nodes) {
			lastPtn++;
			if (lastPtn >= maxPtn)
				lastPtn = 0;
			Integer color = lastPtn;
			tempNode.getProperties().put("color", color);
		}

		return lastPtn;
	}

	private void initPtnAsSingle(ArrayList<NodeData> nodes, Integer defaultPtn) {

		for (NodeData tempNode : nodes) {
			Integer color = new Integer(defaultPtn);
			tempNode.getProperties().put("color", color);
		}

	}

	private void storeRelsToNeo(File graphFile, ChacoParser parser)
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
				flushRelsBatch(nodesAndRels);
				nodesAndRels.clear();
			}
		}

		flushRelsBatch(nodesAndRels);
		nodesAndRels.clear();

		scanner.close();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

	}

	private void storePartitionedRelsToNeo(File graphFile, ChacoParser parser)
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
				flushRelsTrans(nodesAndRels);
				nodesAndRels.clear();
			}
		}
		flushRelsTrans(nodesAndRels);
		nodesAndRels.clear();

		scanner.close();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

	}

	private void flushNodesBatch(ArrayList<NodeData> nodes) {

		for (NodeData nodeAndRels : nodes) {
			long nodeID = batchNeo.createNode(nodeAndRels.getProperties());

			batchIndexService.index(nodeID, "name", nodeAndRels.getProperties()
					.get("name"));
			batchIndexService.index(nodeID, "weight", nodeAndRels
					.getProperties().get("weight"));
			batchIndexService.index(nodeID, "color", nodeAndRels
					.getProperties().get("color"));
		}
	}

	private void flushRelsBatch(ArrayList<NodeData> nodes) {

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
						DynamicRelationshipType.withName("INTERNAL"), rel);
			}
		}
	}

	private void flushRelsTrans(ArrayList<NodeData> nodes) {
		Transaction tx = transNeo.beginTx();

		try {

			for (NodeData nodeAndRels : nodes) {
				String fromName = (String) nodeAndRels.getProperties().get(
						"name");
				Node fromNode = transIndexService.getSingleNode("name",
						fromName);
				Integer fromColor = (Integer) fromNode.getProperty("color");

				for (Map<String, Object> rel : nodeAndRels.getRelationships()) {
					String toName = (String) rel.get("name");
					Node toNode = transIndexService.getSingleNode("name",
							toName);
					Integer toColor = (Integer) toNode.getProperty("color");

					rel.put("name", fromName + "->" + toName);

					if (fromColor == toColor) {
						fromNode.createRelationshipTo(toNode,
								DynamicRelationshipType.withName("INTERNAL"));
					} else {
						fromNode.createRelationshipTo(toNode,
								DynamicRelationshipType.withName("EXTERNAL"));
					}

				}
			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}
	}

	private void openBatchServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Batch Services...");

		batchNeo = new BatchInserterImpl(this.databaseDir, BatchInserterImpl
				.loadProperties("neo.props"));

		batchIndexService = new LuceneIndexBatchInserterImpl(batchNeo);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private void closeBatchServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Closing Batch Services...");

		batchIndexService.shutdown();
		batchNeo.shutdown();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private void openTransServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Transactional Services...");

		transNeo = new EmbeddedGraphDatabase(this.databaseDir);
		transIndexService = new LuceneIndexService(transNeo);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private void closeTransServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Closing Transactional Services...");

		transIndexService.shutdown();
		transNeo.shutdown();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

}