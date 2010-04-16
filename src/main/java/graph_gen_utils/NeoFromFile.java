package graph_gen_utils;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.Consts;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.memory_graph.MemNode;
import graph_gen_utils.memory_graph.MemRel;
import graph_gen_utils.metrics.MetricsWriterUndirected;
import graph_gen_utils.partitioner.Partitioner;
import graph_gen_utils.partitioner.PartitionerAsFile;
import graph_gen_utils.partitioner.PartitionerAsSingle;
import graph_gen_utils.reader.GraphReader;
import graph_gen_utils.reader.chaco.ChacoParserFactory;
import graph_gen_utils.reader.gml.GMLParserDirected;
import graph_gen_utils.reader.topology.GraphTopology;
import graph_gen_utils.reader.topology.GraphTopologyFullyConnected;
import graph_gen_utils.reader.topology.GraphTopologyRandom;
import graph_gen_utils.writer.GraphWriter;
import graph_gen_utils.writer.chaco.ChacoPtnWriterFactory;
import graph_gen_utils.writer.chaco.ChacoWriterFactory;
import graph_gen_utils.writer.gml.GMLWriterUndirectedBasic;
import graph_gen_utils.writer.gml.GMLWriterUndirectedFull;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;

import pGraphService.PGraphDatabaseService;

/**
 * Provides easy means of creating a Neo4j instance from various graph file
 * formats, loading a Neo4j instance into an in-memory graph, calculating
 * various graph metrics and writing them to file.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class NeoFromFile {

	public enum ChacoType {
		UNWEIGHTED, WEIGHTED_EDGES, WEIGHTED_NODES, WEIGHTED
	}

	public static void main(String[] args) throws Exception {

	}

	// **************
	// PUBLIC METHODS
	// **************

	/**
	 * Allocates nodes of a Neo4j instance to clusters/partitions. Allocation is
	 * defined by the allocation of the {@link MemGraph} input parameter.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param memGraph
	 *            instance of {@link MemGraph} that contains cluster/partition
	 *            allocation details
	 */
	public static void updateNeoPtnFromMemGraph(GraphDatabaseService transNeo,
			MemGraph memGraph) {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Updating Neo4j instance...");

		int lastVisitedIndex = -1;
		int datasetSize = 0;
		for (Node tempNode : memGraph.getAllNodes())
			datasetSize++;

		while (lastVisitedIndex < datasetSize - 1) {

			Transaction tx = transNeo.beginTx();

			try {

				int currentIndex = -1;
				int transBuffer = 0;

				for (Node memV : memGraph.getAllNodes()) {

					currentIndex++;

					if (currentIndex < lastVisitedIndex)
						continue;

					lastVisitedIndex = currentIndex;
					transBuffer++;

					Node v = transNeo.getNodeById((Long) memV
							.getProperty(Consts.LID));

					v.setProperty(Consts.COLOR, memV.getProperty(Consts.COLOR));

					// Periodic flush to reduce memory consumption
					if (transBuffer % Consts.STORE_BUF == 0) {
						// Commit transaction
						break;
					}

				}

				tx.success();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}
		}

		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

	}

	/**
	 * Allocates nodes of a Neo4j instance to clusters/partitions. Allocation
	 * scheme is defined by the {@link Partitioner} parameter.
	 * 
	 * Method writes {@link Consts#COLOR} property to all nodes of an existing
	 * Neo4j instance. {@link Consts#GID} property is also written to all nodes
	 * and set to {@link Node#getId()}.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 * 
	 * @throws Exception
	 */
	public static void applyPtnToNeo(GraphDatabaseService transNeo,
			Partitioner partitioner) throws Exception {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Loading Neo4j instance...");

		ArrayList<ArrayList<NodeData>> nodesCollection = new ArrayList<ArrayList<NodeData>>();
		ArrayList<NodeData> nodes = null;

		Transaction tx = transNeo.beginTx();

		try {

			Integer nodeNumber = 0;

			for (Node node : transNeo.getAllNodes()) {

				if ((nodes == null) || (nodes.size() % Consts.STORE_BUF == 0)) {
					nodes = new ArrayList<NodeData>();
					nodesCollection.add(nodes);
				}

				nodeNumber++;

				NodeData nodeData = new NodeData();

				Double lat = 0.0;
				if (node.hasProperty(Consts.LATITUDE))
					lat = (Double) node.getProperty(Consts.LATITUDE);

				Double lon = 0.0;
				if (node.hasProperty(Consts.LONGITUDE))
					lon = (Double) node.getProperty(Consts.LONGITUDE);

				nodeData.getProperties().put(Consts.LATITUDE, lat);
				nodeData.getProperties().put(Consts.LONGITUDE, lon);
				nodeData.getProperties().put(Consts.LID, node.getId());
				nodeData.getProperties().put(Consts.GID, nodeNumber.toString());

				nodes.add(nodeData);

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
		time = System.currentTimeMillis();
		System.out.printf("Partitioning Neo4j instance...");

		IndexService transIndexService = new LuceneIndexService(transNeo);

		// Apply partitioning and properties
		for (ArrayList<NodeData> nodesBuff : nodesCollection) {
			nodesBuff = partitioner.applyPartitioning(nodesBuff);
			applyNodeProps(transIndexService, transNeo, nodesBuff);
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

	}

	/**
	 * Creates a Neo4j instance and populates it according to a generated graph
	 * topology. Examples of possible topologies are random (
	 * {@link GraphTopologyRandom}) and fully connected (
	 * {@link GraphTopologyFullyConnected}) graphs.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param topology
	 *            instance of {@link GraphTopology} the defines generated
	 *            topology
	 * 
	 * @throws Exception
	 */
	public static void writeNeoFromTopology(GraphDatabaseService transNeo,
			GraphTopology topology) throws Exception {

		Partitioner partitioner = new PartitionerAsSingle((byte) -1);

		storePartitionedNodesAndRelsToNeo(transNeo, topology, partitioner);

	}

	/**
	 * Creates a Neo4j instance, populates it according to a generated graph
	 * topology, then allocates {@link Node}s to partitions/clusters.
	 * 
	 * Examples of possible topologies are random ({@link GraphTopologyRandom})
	 * and fully connected ({@link GraphTopologyFullyConnected}) graphs.
	 * 
	 * Allocation scheme is defined by the {@link Partitioner} parameter.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param topology
	 *            instance of {@link GraphTopology} the defines generated
	 *            topology
	 * 
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 * 
	 * @throws Exception
	 */
	public static void writeNeoFromTopologyAndPtn(
			GraphDatabaseService transNeo, GraphTopology topology,
			Partitioner partitioner) throws Exception {

		storePartitionedNodesAndRelsToNeo(transNeo, topology, partitioner);

	}

	/**
	 * Creates a Neo4j instance and populates it from the contents of a Chaco
	 * (.graph) file. Chaco files are basically persistent adjacency lists.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 * 
	 * @throws Exception
	 */
	public static void writeNeoFromChaco(GraphDatabaseService transNeo,
			String graphPath) throws Exception {

		GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
		Partitioner partitioner = new PartitionerAsSingle((byte) -1);

		storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);

	}

	/**
	 * Creates a Neo4j instance, populates it from the contents of a Chaco
	 * (.graph) file, then allocates {@link Node}s to partitions/clusters.
	 * 
	 * Chaco files are basically persistent adjacency lists.
	 * 
	 * Partition/cluster allocation is defined by the contents of a .ptn file.
	 * 
	 * This method is only included for convenience/ease of use.
	 * {@link NeoFromFile#writeNeoFromChacoAndPtn(GraphDatabaseService, String, Partitioner)}
	 * can achieve the same thing.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 * 
	 * @param ptnPath
	 *            {@link String} representing path to .ptn file
	 * 
	 * @throws Exception
	 */
	public static void writeNeoFromChacoAndPtn(GraphDatabaseService transNeo,
			String graphPath, String ptnPath) throws Exception {

		GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
		Partitioner partitioner = new PartitionerAsFile(new File(ptnPath));

		storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);

	}

	/**
	 * Creates a Neo4j instance, populates it from the contents of a Chaco
	 * (.graph) file, then allocates {@link Node}s to partitions/clusters.
	 * 
	 * Chaco files are basically persistent adjacency lists.
	 * 
	 * Allocation scheme is defined by the {@link Partitioner} parameter.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 * 
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 * 
	 * @throws Exception
	 */
	public static void writeNeoFromChacoAndPtn(GraphDatabaseService transNeo,
			String graphPath, Partitioner partitioner) throws Exception {

		GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);

		storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);

	}

	/**
	 * Creates a Neo4j instance and populates it from the contents of a GML
	 * (.gml) file.
	 * 
	 * GML files are basically an ASCII version of the GraphML format.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 * 
	 * @throws Exception
	 */
	public static void writeNeoFromGML(GraphDatabaseService transNeo,
			String gmlPath) throws Exception {

		GraphReader parser = new GMLParserDirected(new File(gmlPath));
		Partitioner partitioner = new PartitionerAsSingle((byte) -1);

		storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);

	}

	/**
	 * Creates a Neo4j instance and populates it from the contents of a GML
	 * (.gml) file, then allocates {@link Node}s to partitions/clusters.
	 * 
	 * GML files are basically an ASCII version of the GraphML format.
	 * 
	 * Allocation scheme is defined by the {@link Partitioner} parameter.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 * 
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 * 
	 * @throws Exception
	 */
	public static void writeNeoFromGMLAndPtn(GraphDatabaseService transNeo,
			String gmlPath, Partitioner partitioner) throws Exception {

		GraphReader parser = new GMLParserDirected(new File(gmlPath));

		storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);

	}

	/**
	 * Creates a Chaco file and populates it with the adjacency list
	 * representation of the current Neo4j instance.
	 * 
	 * Chaco files are assumed to be undirected, this means edges are duplicated
	 * in each direction.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param chacoPath
	 *            {@link String} representing path to .graph file
	 * 
	 * @param chacoType
	 *            {@link ChacoType} specifies whether node and/or edge weights
	 *            are written to the chaco file
	 * 
	 * @throws Exception
	 */
	public static void writeChaco(GraphDatabaseService transNeo,
			String chacoPath, ChacoType chacoType) throws Exception {

		File chacoFile = new File(chacoPath);

		GraphWriter chacoWriter = ChacoWriterFactory.getChacoWriter(chacoType,
				chacoFile);

		writeGraphToFile(transNeo, chacoWriter);

	}

	/**
	 * Creates a Chaco (.graph) file and partition (.ptn) files, populates them
	 * with the representation of the current Neo4j instance.
	 * 
	 * Chaco files are assumed to be undirected, this means edges are duplicated
	 * in each direction.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param chacoPath
	 *            {@link String} representing path to .graph file
	 * 
	 * @param chacoType
	 *            {@link ChacoType} specifies whether node and/or edge weights
	 *            are written to the chaco file
	 * 
	 * @param ptnPath
	 *            {@link String} representing path to .ptn file
	 * 
	 * @throws Exception
	 */
	public static void writeChacoAndPtn(GraphDatabaseService transNeo,
			String chacoPath, ChacoType chacoType, String ptnPath)
			throws Exception {

		File chacoFile = new File(chacoPath);
		File ptnFile = new File(ptnPath);

		GraphWriter chacoPtnWriter = ChacoPtnWriterFactory.getChacoPtnWriter(
				chacoType, chacoFile, ptnFile);

		writeGraphToFile(transNeo, chacoPtnWriter);

	}

	/**
	 * Creates a GML (.gml) file and populates it with the representation of the
	 * current Neo4j instance.
	 * 
	 * All {@link Node} and {@link Relationship} properties are written to the
	 * GML file.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 * 
	 * @throws Exception
	 */
	public static void writeGMLFull(GraphDatabaseService transNeo,
			String gmlPath) throws Exception {

		File gmlFile = new File(gmlPath);

		GraphWriter gmlWriterFull = new GMLWriterUndirectedFull(gmlFile);

		writeGraphToFile(transNeo, gmlWriterFull);

	}

	/**
	 * Creates a GML (.gml) file and populates it with the representation of the
	 * current Neo4j instance.
	 * 
	 * Only certain {@link Node} and {@link Relationship} properties are written
	 * to the GML file. {@link Consts#COLOR}, {@link Consts#WEIGHT},
	 * {@link Consts#GID}, and {@link Consts#LID}.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 * 
	 * @throws Exception
	 */
	public static void writeGMLBasic(GraphDatabaseService transNeo,
			String gmlPath) throws Exception {

		File gmlFile = new File(gmlPath);

		GraphWriter gmlWriterBasic = new GMLWriterUndirectedBasic(gmlFile);

		writeGraphToFile(transNeo, gmlWriterBasic);

	}

	/**
	 * Calculates graph metrics for the current Neo4j instance, creates a
	 * metrics (.met) file and populates it.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param metricsPath
	 *            {@link String} representing path to .met file
	 * 
	 * @throws Exception
	 */
	public static void writeMetrics(GraphDatabaseService transNeo,
			String metricsPath) throws Exception {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Metrics File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUndirected.writeMetrics(transNeo, metricsFile);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

	}

	/**
	 * Calculates graph metrics for the current Neo4j instance, creates a comma
	 * separated metrics (.met) file and populates it.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param metricsPath
	 *            {@link String} representing path to .met file
	 * 
	 * @throws Exception
	 */
	public static void writeMetricsCSV(GraphDatabaseService transNeo,
			String metricsPath) throws Exception {
		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Metrics CSV File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUndirected.writeMetricsCSV(transNeo, metricsFile, null);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
	}

	/**
	 * Calculates graph metrics for the current Neo4j instance and appends the
	 * results to a comma separated metrics (.met) file.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param metricsPath
	 *            {@link String} representing path to .met file
	 * 
	 * @param timeStep
	 *            {@link Long} representing the time-step/iteration related to
	 *            these metrics
	 * 
	 * @throws Exception
	 */
	public static void appendMetricsCSV(GraphDatabaseService transNeo,
			String metricsPath, Long timeStep) throws Exception {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Appending Metrics CSV File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUndirected.appendMetricsCSV(transNeo, metricsFile,
				timeStep);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

	}

	/**
	 * Loads the current Neo4j instance into an in-memory graph.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @return {@link MemGraph}
	 */
	public static MemGraph readMemGraph(GraphDatabaseService transNeo) {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Loading Neo4j into MemGraph...");

		MemGraph memGraph = new MemGraph();

		Transaction tx = transNeo.beginTx();

		long edgeCount = 0;
		double minWeight = Double.MAX_VALUE;
		double maxWeight = Double.MIN_VALUE;
		double totalWeight = 0.0;
		double normalizedMinWeight = Consts.MIN_EDGE_WEIGHT;
		double normalizedMaxWeight = Double.MIN_VALUE;

		try {
			for (Node node : transNeo.getAllNodes()) {

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {

					edgeCount++;

					if (rel.hasProperty(Consts.WEIGHT)) {
						double weight = (Double) rel.getProperty(Consts.WEIGHT);
						if (weight > maxWeight)
							maxWeight = weight;
						if (weight < minWeight)
							minWeight = weight;
						totalWeight += weight;
					}

				}

			}

			normalizedMinWeight = minWeight / maxWeight;

			for (Node node : transNeo.getAllNodes()) {
				Long nodeId = Long.parseLong((String) node
						.getProperty(Consts.GID));

				memGraph.setNextId(nodeId);
				MemNode memNode = (MemNode) memGraph.createNode();
				memNode.setProperty(Consts.GID, nodeId.toString());
				memNode.setProperty(Consts.COLOR, (Byte) node
						.getProperty(Consts.COLOR));
				memNode.setProperty(Consts.LID, node.getId());

			}

			for (Node node : transNeo.getAllNodes()) {

				Long nodeId = Long.parseLong((String) node
						.getProperty(Consts.GID));

				MemNode memNode = (MemNode) memGraph.getNodeById(nodeId);

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {

					// Store normalized edge weight, [0,1]
					double weight = normalizedMinWeight;
					if (rel.hasProperty(Consts.WEIGHT))
						weight = (Double) rel.getProperty(Consts.WEIGHT)
								/ maxWeight;

					Long startNodeId = Long.parseLong((String) rel
							.getStartNode().getProperty(Consts.GID));
					Long endNodeId = Long.parseLong((String) rel.getEndNode()
							.getProperty(Consts.GID));

					MemRel memRel = (MemRel) memNode.tryGetRelationship(
							startNodeId, endNodeId);
					if (memRel == null) {
						MemNode endNode = (MemNode) memGraph
								.getNodeById(endNodeId);
						memRel = (MemRel) memNode.createRelationshipTo(endNode,
								Consts.DEFAULT_REL_TYPE);
						memRel.setProperty(Consts.WEIGHT, weight);
					} else {
						weight = weight
								+ (Double) memRel.getProperty(Consts.WEIGHT);
						memRel.setProperty(Consts.WEIGHT, weight);
					}

					if (weight > normalizedMaxWeight)
						normalizedMaxWeight = weight;
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// TODO For prinout only. Remove later!
		minWeight = Double.MAX_VALUE;
		maxWeight = Double.MIN_VALUE;
		edgeCount = 0;
		long nodeCount = 0;

		// Normalize edge weights again
		// If multiple edges exist between same 2 vertices, weight>1 is possible
		for (Node memV : memGraph.getAllNodes()) {
			nodeCount++;
			for (Relationship memR : memV.getRelationships(Direction.OUTGOING)) {

				double weight = (Double) memR.getProperty(Consts.WEIGHT)
						/ normalizedMaxWeight;

				// Consider all edges to a certain extent
				if (weight < Consts.MIN_EDGE_WEIGHT)
					weight = Consts.MIN_EDGE_WEIGHT;

				memR.setProperty(Consts.WEIGHT, weight);

				// TODO For prinout only. Remove later!
				if (weight > maxWeight)
					maxWeight = weight;
				if (weight < minWeight)
					minWeight = weight;
				edgeCount++;
			}
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		System.out.printf("\tNode Count = %d\n", nodeCount);
		System.out.printf("\tEdge Count = %d\n", edgeCount);
		System.out.printf("\tMin Edge Weight = %f\n", minWeight);
		System.out.printf("\tMax Edge Weight = %f\n", maxWeight);

		return memGraph;

	}

	// **************
	// PRIVATE METHODS
	// ***************

	private static void writeGraphToFile(GraphDatabaseService transNeo,
			GraphWriter graphWriter) throws Exception {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Neo4j to File...");

		graphWriter.write(transNeo);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

	}

	private static void storePartitionedNodesAndRelsToNeo(
			GraphDatabaseService transNeo, GraphReader parser,
			Partitioner partitioner) throws Exception {

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Reading & Indexing Nodes...");

		IndexService transIndexService = new LuceneIndexService(transNeo);

		ArrayList<NodeData> nodesAndRels = new ArrayList<NodeData>();

		for (NodeData nodeData : parser.getNodes()) {
			nodesAndRels.add(nodeData);

			if ((nodesAndRels.size() % Consts.STORE_BUF) == 0) {

				// PRINTOUT
				System.out.printf(".");

				nodesAndRels = partitioner.applyPartitioning(nodesAndRels);
				flushNodesTrans(transIndexService, transNeo, nodesAndRels);
				nodesAndRels.clear();
			}
		}

		nodesAndRels = partitioner.applyPartitioning(nodesAndRels);
		flushNodesTrans(transIndexService, transNeo, nodesAndRels);
		nodesAndRels.clear();

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
		time = System.currentTimeMillis();
		System.out.printf("Optimizing Index...");

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		// PRINTOUT
		System.out.printf("Reading & Indexing Relationships...");

		time = System.currentTimeMillis();

		for (NodeData nodeData : parser.getRels()) {
			nodesAndRels.add(nodeData);

			if ((nodesAndRels.size() % Consts.STORE_BUF) == 0) {

				// PRINTOUT
				System.out.printf(".");

				flushRelsTrans(transIndexService, transNeo, nodesAndRels);
				nodesAndRels.clear();
			}
		}

		flushRelsTrans(transIndexService, transNeo, nodesAndRels);
		nodesAndRels.clear();

		removeReferenceNode(transNeo);

		transIndexService.shutdown();

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

	}

	private static void flushNodesTrans(IndexService transIndexService,
			GraphDatabaseService transNeo, ArrayList<NodeData> nodes) {

		Transaction tx = transNeo.beginTx();

		try {
			for (NodeData nodeAndRels : nodes) {
				Node node = transNeo.createNode();

				for (Entry<String, Object> nodeProp : nodeAndRels
						.getProperties().entrySet()) {

					node.setProperty(nodeProp.getKey(), nodeProp.getValue());
					transIndexService.index(node, nodeProp.getKey(), nodeProp
							.getValue());

				}

			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

	}

	private static void flushRelsTrans(IndexService transIndexService,
			GraphDatabaseService transNeo, ArrayList<NodeData> nodes) {

		Transaction tx = transNeo.beginTx();

		String fromName = null;
		String toName = null;

		try {

			for (NodeData nodeAndRels : nodes) {
				fromName = (String) nodeAndRels.getProperties().get(Consts.GID);
				Node fromNode = transIndexService.getSingleNode(Consts.GID,
						fromName);
				Byte fromColor = (Byte) fromNode.getProperty(Consts.COLOR);

				for (Map<String, Object> rel : nodeAndRels.getRelationships()) {
					toName = (String) rel.get(Consts.GID);

					Node toNode = transIndexService.getSingleNode(Consts.GID,
							toName);
					Byte toColor = (Byte) toNode.getProperty(Consts.COLOR);

					Relationship neoRel = null;

					if (fromColor == toColor) {
						neoRel = fromNode.createRelationshipTo(toNode,
								DynamicRelationshipType.withName("INTERNAL"));
					} else {
						neoRel = fromNode.createRelationshipTo(toNode,
								DynamicRelationshipType.withName("EXTERNAL"));
					}

					for (Entry<String, Object> relProp : rel.entrySet()) {

						String relPropKey = relProp.getKey();

						if (relPropKey.equals(Consts.GID))
							continue;

						neoRel.setProperty(relPropKey, relProp.getValue());

					}

				}
			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.printf("%nfromName[%s] toName[%s]%n", fromName, toName);
		} finally {
			tx.finish();
		}
	}

	private static void applyNodeProps(IndexService transIndexService,
			GraphDatabaseService transNeo, ArrayList<NodeData> nodes) {
		Transaction tx = transNeo.beginTx();

		try {

			for (NodeData nodeAndRels : nodes) {
				Long nodeId = (Long) nodeAndRels.getProperties()
						.get(Consts.LID);
				Node node = transNeo.getNodeById(nodeId);

				for (Entry<String, Object> prop : nodeAndRels.getProperties()
						.entrySet()) {

					if (prop.getKey().equals(Consts.LID))
						continue;

					node.setProperty(prop.getKey(), prop.getValue());

					transIndexService.index(node, prop.getKey(), prop
							.getValue());

				}
			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}
	}

	// Used when reading from input graph (E.g. Chaco, GML, Topology etc)
	// Not used when working on existing Neo4j instance
	private static void removeReferenceNode(GraphDatabaseService transNeo) {
		Transaction tx = transNeo.beginTx();

		try {
			Node refNode = transNeo.getReferenceNode();

			for (Relationship refRel : refNode.getRelationships())
				refRel.delete();

			refNode.delete();
			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}
	}

	private static boolean isSupportedGraphDatabaseService(
			GraphDatabaseService transNeo) {
		if (transNeo instanceof MemGraph)
			return false;
		if (transNeo instanceof PGraphDatabaseService)
			return false;
		return true;
	}

	private static String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}

}