package graph_gen_utils;

import graph_gen_utils.chaco.ChacoParser;
import graph_gen_utils.chaco.ChacoParserUnweighted;
import graph_gen_utils.chaco.ChacoParserWeighted;
import graph_gen_utils.chaco.ChacoParserWeightedEdges;
import graph_gen_utils.chaco.ChacoParserWeightedNodes;
import graph_gen_utils.chaco.ChacoWriter;
import graph_gen_utils.chaco.ChacoWriterUnweighted;
import graph_gen_utils.general.NodeData;
import graph_gen_utils.gml.GMLParser;
import graph_gen_utils.gml.GMLParserUndirected;
import graph_gen_utils.gml.GMLWriter;
import graph_gen_utils.gml.GMLWriterUndirected;
import graph_gen_utils.graph.MemGraph;
import graph_gen_utils.graph.MemRel;
import graph_gen_utils.metrics.MetricsWriterUnweighted;
import graph_gen_utils.topology.GraphTopology;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
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

		NeoFromFile neoCreator = new NeoFromFile("var/big-random");

		neoCreator.writeNeoFromGML("temp/big-random.gml");
		neoCreator.writeChaco("temp/big-random.graph", ChacoType.UNWEIGHTED);

		// NeoFromFile neoCreatorBefore = new NeoFromFile("var/test0-before");
		// NeoFromFile neoCreatorAfter = new NeoFromFile("var/test0-after");
		//
		// neoCreatorBefore.writeNeoFromChaco("graphs/test0.graph");
		// neoCreatorBefore.writeGML("temp/test0-before.gml");
		//
		// neoCreatorAfter.writeNeoFromGML("temp/test0-before.gml");
		// neoCreatorAfter.writeGML("temp/test0-after.gen.gml");
		// neoCreatorAfter.writeChaco("temp/test0-after.graph",
		// ChacoType.UNWEIGHTED);

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

	// **************
	// PUBLIC METHODS
	// **************

	public void writeNeoFromTopology(GraphTopology topology) throws Exception {

		storePartitionedNodesAndRelsToNeo(topology, ClusterInitType.SINGLE,
				(byte) -1);

	}

	public void writeNeoFromTopology(GraphTopology topology,
			ClusterInitType clusterInitType, byte ptnVal) throws Exception {

		storePartitionedNodesAndRelsToNeo(topology, clusterInitType, ptnVal);

	}

	public void writeNeoFromChaco(String graphPath) throws Exception {

		writeNeoFromChaco(graphPath, ClusterInitType.SINGLE, (byte) -1);

	}

	public void writeNeoFromChaco(String graphPath,
			ClusterInitType clusterInitType, byte ptnVal) throws Exception {

		openBatchServices();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Graph File...");

		File graphFile = new File(graphPath);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		ChacoParser parser = getChacoParser(graphFile);

		storePartitionedNodesToNeo(graphFile, clusterInitType, ptnVal, parser);

		closeBatchServices();

		openTransServices();

		storePartitionedRelsToNeo(graphFile, parser);

		closeTransServices();

	}

	public void writeNeoFromChaco(String graphPath, String partitionPath)
			throws FileNotFoundException {

		openBatchServices();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Graph & Partitioning Files...");

		File graphFile = new File(graphPath);
		File partitionFile = new File(partitionPath);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		ChacoParser parser = getChacoParser(graphFile);

		storePartitionedNodesToNeo(graphFile, partitionFile, parser);

		closeBatchServices();

		openTransServices();

		storePartitionedRelsToNeo(graphFile, parser);

		closeTransServices();

	}

	public void writeNeoFromGML(String gmlPath) throws Exception {

		GMLParser parser = new GMLParserUndirected(new File(gmlPath));

		storeNodesAndRelsToNeo(parser);

	}

	public void writeChaco(String chacoPath, ChacoType chacoType)
			throws Exception {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Chaco File...");

		File chacoFile = null;
		ChacoWriter chacoWriter = getChacoWriter(chacoType);

		chacoFile = new File(chacoPath);

		chacoWriter.write(transNeo, chacoFile);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();

	}

	public void writeChacoAndPtn(String chacoPath, ChacoType chacoType,
			String ptnPath) throws Exception {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Chaco & Partition Files...");

		File chacoFile = null;
		File ptnFile = null;

		ChacoWriter chacoWriter = getChacoWriter(chacoType);

		chacoFile = new File(chacoPath);
		ptnFile = new File(ptnPath);

		chacoWriter.write(transNeo, chacoFile, ptnFile);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();
	}

	public void writeGML(String gmlPath) throws Exception {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing GML File...");

		File gmlFile = null;
		GMLWriter gmlWriter = new GMLWriterUndirected();

		gmlFile = new File(gmlPath);

		gmlWriter.write(transNeo, gmlFile);

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
		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Metrics CSV File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUnweighted.writeMetricsCSV(transNeo, metricsFile, null);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();
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

	public MemGraph readMemGraph() {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Loading Neo4j into MemGraph...");

		MemGraph memGraph = new MemGraph();

		Transaction tx = transNeo.beginTx();

		try {
			for (Node node : this.transNeo.getAllNodes()) {
				// Ignore reference node
				if (node.getId() == 0)
					continue;

				Long nodeId = Long.parseLong((String) node.getProperty("name"));

				memGraph.addNode(nodeId, (Byte) node.getProperty("color"));

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {
					MemRel memRel = new MemRel(rel.getEndNode().getId(),
							(Double) rel.getProperty("weight"));

					memGraph.getNode(nodeId).addNeighbour(memRel);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);

		closeTransServices();

		return memGraph;
	}

	// **************
	// PRIVATE METHODS
	// ***************

	private ChacoWriter getChacoWriter(ChacoType chacoType) throws Exception {
		switch (chacoType) {
		case UNWEIGHTED:
			return new ChacoWriterUnweighted();
		case WEIGHTED_EDGES:
			throw new Exception("ChacoType[WEIGHTED_EDGES] supported yet");
		case WEIGHTED_NODES:
			throw new Exception("ChacoType[WEIGHTED_NODES] supported yet");
		case WEIGHTED:
			throw new Exception("ChacoType[WEIGHTED] supported yet");
		default:
			throw new Exception("ChacoType not recognized");
		}
	}

	private ChacoParser getChacoParser(File graphFile)
			throws FileNotFoundException {

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

	private void storePartitionedNodesAndRelsToNeo(GraphTopology topology,
			ClusterInitType clusterInitType, byte ptnVal) throws Exception {

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
			initPtnAsBalanced(nodesAndRels, (byte) -1, ptnVal);
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

	private void storeNodesAndRelsToNeo(GMLParser parser) throws Exception {

		openBatchServices();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Reading & Indexing Nodes...");

		ArrayList<NodeData> nodesAndRels = new ArrayList<NodeData>();

		for (NodeData nodeData : parser.getNodes()) {
			nodesAndRels.add(nodeData);

			if ((nodesAndRels.size() % NeoFromFile.NODE_STORE_BUF) == 0) {

				// PRINTOUT
				System.out.printf(".");

				flushNodesBatch(nodesAndRels);
				nodesAndRels.clear();
			}
		}

		flushNodesBatch(nodesAndRels);
		nodesAndRels.clear();

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

		for (NodeData nodeData : parser.getRels()) {
			nodesAndRels.add(nodeData);

			if ((nodesAndRels.size() % NeoFromFile.REL_STORE_BUF) == 0) {

				// PRINTOUT
				System.out.printf(".");

				flushRelsTrans(nodesAndRels);
				nodesAndRels.clear();
			}
		}

		flushRelsTrans(nodesAndRels);
		nodesAndRels.clear();

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
			ClusterInitType clusterInitType, byte ptnVal, ChacoParser parser)
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
		byte lastPtn = -1;
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

	// TODO encapsulate in class InitPartition, InitPartitionAsFile
	private void initPtnAsFile(Scanner partitionScanner,
			ArrayList<NodeData> nodes) {

		for (NodeData tempNode : nodes) {
			Byte color = Byte.parseByte(partitionScanner.nextLine());
			tempNode.getProperties().put("color", color);
		}

	}

	// TODO encapsulate in class InitPartition, InitPartitionAsRandom
	private void initPtnAsRandom(ArrayList<NodeData> nodes, byte maxPtn) {

		Random rand = new Random(System.currentTimeMillis());

		for (NodeData tempNode : nodes) {
			Byte color = (byte) rand.nextInt(maxPtn);
			tempNode.getProperties().put("color", color);
		}

	}

	// TODO encapsulate in class InitPartition, InitPartitionAsBalanced
	private byte initPtnAsBalanced(ArrayList<NodeData> nodes, byte lastPtn,
			byte maxPtn) {

		for (NodeData tempNode : nodes) {
			lastPtn++;
			if (lastPtn >= maxPtn)
				lastPtn = 0;
			Byte color = lastPtn;
			tempNode.getProperties().put("color", color);
		}

		return lastPtn;
	}

	// TODO encapsulate in class InitPartition, InitPartitionAsSingle
	private void initPtnAsSingle(ArrayList<NodeData> nodes, byte defaultPtn) {

		for (NodeData tempNode : nodes) {
			Byte color = new Byte(defaultPtn);
			tempNode.getProperties().put("color", color);
		}

	}

	private void flushNodesBatch(ArrayList<NodeData> nodes) {

		for (NodeData nodeAndRels : nodes) {
			long nodeID = batchNeo.createNode(nodeAndRels.getProperties());

			for (Entry<String, Object> nodeProp : nodeAndRels.getProperties()
					.entrySet()) {

				batchIndexService.index(nodeID, nodeProp.getKey(), nodeProp
						.getValue());

			}

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

		String fromName = null;
		String toName = null;

		try {

			for (NodeData nodeAndRels : nodes) {
				fromName = (String) nodeAndRels.getProperties().get("name");
				Node fromNode = transIndexService.getSingleNode("name",
						fromName);
				Byte fromColor = (Byte) fromNode.getProperty("color");

				for (Map<String, Object> rel : nodeAndRels.getRelationships()) {
					toName = (String) rel.get("name");

					// System.out.printf("%nfromName[%s] toName[%s]%n",
					// fromName,
					// toName);

					Node toNode = transIndexService.getSingleNode("name",
							toName);
					Byte toColor = (Byte) toNode.getProperty("color");

					Relationship neoRel = null;

					if (fromColor == toColor) {
						neoRel = fromNode.createRelationshipTo(toNode,
								DynamicRelationshipType.withName("INTERNAL"));
					} else {
						neoRel = fromNode.createRelationshipTo(toNode,
								DynamicRelationshipType.withName("EXTERNAL"));
					}

					// neoRel.setProperty("name", fromName + "->" + toName);

					for (Entry<String, Object> relProp : rel.entrySet()) {

						String relPropKey = relProp.getKey();

						if (relPropKey.equals("name"))
							continue;

						neoRel.setProperty(relPropKey, relProp.getValue());

					}

				}
			}

			tx.success();
		} catch (Exception e) {
			System.out.printf("%nfromName[%s] toName[%s]%n", fromName, toName);
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