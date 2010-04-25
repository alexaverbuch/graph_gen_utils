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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;

import p_graph_service.PGraphDatabaseService;

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
		Set<String> x = new HashSet<String>();
		x.add("cow");
		System.out.println("Add 'cow'");
		x.add("chicken");
		System.out.println("Add 'chicken'");
		System.out.printf("Contains 'cow' == %b\n", x.contains("cow"));
		System.out.printf("Contains 'horse' == %b\n", x.contains("horse"));
	}

	// **************
	// PUBLIC METHODS
	// **************

	/**
	 * Allocates nodes of a Neo4j instance to clusters/partitions. Allocation
	 * scheme is defined by the {@link Partitioner} parameter.
	 * 
	 * Method writes {@link Consts#COLOR} property to all nodes of an existing
	 * Neo4j instance. {@link Consts#NODE_LID}, {@link Consts#NODE_GID},
	 * {@link Consts#LATITUDE},{@link Consts#LONGITUDE} properties are also
	 * written to all nodes.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public static void applyPtnToNeo(GraphDatabaseService transNeo,
			Partitioner partitioner) {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Loading Neo4j instance...");

		ArrayList<ArrayList<NodeData>> nodesCollection = new ArrayList<ArrayList<NodeData>>();
		ArrayList<NodeData> nodes = null;

		Transaction tx = transNeo.beginTx();

		try {

			Long nodeNumber = new Long(0);

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
				nodeData.getProperties().put(Consts.NODE_LID, node.getId());
				nodeData.getProperties().put(Consts.NODE_GID, nodeNumber);

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

		transIndexService.shutdown();

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
	 */
	public static void writeNeoFromTopology(GraphDatabaseService transNeo,
			GraphTopology topology) {

		try {
			Partitioner partitioner = new PartitionerAsSingle((byte) -1);
			storePartitionedNodesAndRelsToNeo(transNeo, topology, partitioner);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeNeoFromTopologyAndPtn(
			GraphDatabaseService transNeo, GraphTopology topology,
			Partitioner partitioner) {

		try {
			storePartitionedNodesAndRelsToNeo(transNeo, topology, partitioner);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeNeoFromChaco(GraphDatabaseService transNeo,
			String graphPath) {

		try {
			GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
			Partitioner partitioner = new PartitionerAsSingle((byte) -1);
			storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeNeoFromChacoAndPtn(GraphDatabaseService transNeo,
			String graphPath, String ptnPath) {

		try {
			GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
			Partitioner partitioner = new PartitionerAsFile(new File(ptnPath));
			storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeNeoFromChacoAndPtn(GraphDatabaseService transNeo,
			String graphPath, Partitioner partitioner) {

		try {
			GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
			storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeNeoFromGML(GraphDatabaseService transNeo,
			String gmlPath) {

		try {
			GraphReader parser = new GMLParserDirected(new File(gmlPath));
			Partitioner partitioner = new PartitionerAsSingle((byte) -1);
			storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeNeoFromGMLAndPtn(GraphDatabaseService transNeo,
			String gmlPath, Partitioner partitioner) {

		try {
			GraphReader parser = new GMLParserDirected(new File(gmlPath));
			storePartitionedNodesAndRelsToNeo(transNeo, parser, partitioner);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeChaco(GraphDatabaseService transNeo,
			String chacoPath, ChacoType chacoType) {

		try {
			File chacoFile = new File(chacoPath);
			GraphWriter chacoWriter = ChacoWriterFactory.getChacoWriter(
					chacoType, chacoFile);
			writeGraphToFile(transNeo, chacoWriter);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeChacoAndPtn(GraphDatabaseService transNeo,
			String chacoPath, ChacoType chacoType, String ptnPath) {

		try {
			File chacoFile = new File(chacoPath);
			File ptnFile = new File(ptnPath);
			GraphWriter chacoPtnWriter = ChacoPtnWriterFactory
					.getChacoPtnWriter(chacoType, chacoFile, ptnFile);
			writeGraphToFile(transNeo, chacoPtnWriter);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeGMLFull(GraphDatabaseService transNeo,
			String gmlPath) {

		try {
			File gmlFile = new File(gmlPath);
			GraphWriter gmlWriterFull = new GMLWriterUndirectedFull(gmlFile);
			writeGraphToFile(transNeo, gmlWriterFull);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a GML (.gml) file and populates it with the representation of the
	 * current Neo4j instance.
	 * 
	 * Only certain {@link Node} and {@link Relationship} properties are written
	 * to the GML file. {@link Consts#COLOR}, {@link Consts#WEIGHT},
	 * {@link Consts#NODE_GID}.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * 
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 */
	public static void writeGMLBasic(GraphDatabaseService transNeo,
			String gmlPath) {

		try {
			File gmlFile = new File(gmlPath);
			GraphWriter gmlWriterBasic = new GMLWriterUndirectedBasic(gmlFile);
			writeGraphToFile(transNeo, gmlWriterBasic);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeMetrics(GraphDatabaseService transNeo,
			String metricsPath) {

		try {
			// PRINTOUT
			long time = System.currentTimeMillis();
			System.out.printf("Writing Metrics File...");

			File metricsFile = new File(metricsPath);
			MetricsWriterUndirected.writeMetrics(transNeo, metricsFile);

			// PRINTOUT
			System.out.printf("%s", getTimeStr(System.currentTimeMillis()
					- time));
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	 */
	public static void writeMetricsCSV(GraphDatabaseService transNeo,
			String metricsPath) {

		try {
			// PRINTOUT
			long time = System.currentTimeMillis();
			System.out.printf("Writing Metrics CSV File...");

			File metricsFile = new File(metricsPath);
			MetricsWriterUndirected
					.writeMetricsCSV(transNeo, metricsFile, null);

			// PRINTOUT
			System.out.printf("%s", getTimeStr(System.currentTimeMillis()
					- time));
		} catch (Exception e) {
			e.printStackTrace();
		}
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
	 */
	public static void appendMetricsCSV(GraphDatabaseService transNeo,
			String metricsPath, Long timeStep) {

		try {
			// PRINTOUT
			long time = System.currentTimeMillis();
			System.out.printf("Appending Metrics CSV File...");

			File metricsFile = new File(metricsPath);
			MetricsWriterUndirected.appendMetricsCSV(transNeo, metricsFile,
					timeStep);

			// PRINTOUT
			System.out.printf("%s", getTimeStr(System.currentTimeMillis()
					- time));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Loads the current Neo4j instance into an in-memory graph. Normalized edge
	 * weights to the range [0,1].
	 * 
	 * 
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

		long nodeCount = 0;
		long edgeCount = 0;
		double minWeight = Double.MAX_VALUE;
		double maxWeight = Double.MIN_VALUE;
		double normalizedMinWeight = Consts.MIN_EDGE_WEIGHT;
		double normalizedMaxWeight = Double.MIN_VALUE;

		try {
			for (Node node : transNeo.getAllNodes()) {

				nodeCount++;

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {

					edgeCount++;

					if (rel.hasProperty(Consts.WEIGHT)) {
						double weight = (Double) rel.getProperty(Consts.WEIGHT);
						if (weight > maxWeight)
							maxWeight = weight;
						if (weight < minWeight)
							minWeight = weight;
					}

				}

			}

			normalizedMinWeight = minWeight / maxWeight;
			if (normalizedMinWeight < Consts.MIN_EDGE_WEIGHT)
				normalizedMinWeight = Consts.MIN_EDGE_WEIGHT;

			for (Node node : transNeo.getAllNodes()) {
				Long nodeGID = (Long) node.getProperty(Consts.NODE_GID);

				memGraph.setNextNodeId(nodeGID);
				MemNode memNode = (MemNode) memGraph.createNode();
				memNode.setProperty(Consts.NODE_GID, nodeGID);

				Byte nodeColor = -1;
				if (node.hasProperty(Consts.COLOR))
					nodeColor = (Byte) node.getProperty(Consts.COLOR);
				memNode.setProperty(Consts.COLOR, nodeColor);

			}

			for (Node node : transNeo.getAllNodes()) {

				Long nodeGID = (Long) node.getProperty(Consts.NODE_GID);

				MemNode memNode = (MemNode) memGraph.getNodeById(nodeGID);

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {

					// Store normalized edge weight, [0,1]
					double weight = normalizedMinWeight;
					if (rel.hasProperty(Consts.WEIGHT)) {
						weight = (Double) rel.getProperty(Consts.WEIGHT)
								/ maxWeight;

						if (weight > normalizedMaxWeight)
							normalizedMaxWeight = weight;

					}

					Long endNodeId = (Long) rel.getEndNode().getProperty(
							Consts.NODE_GID);

					MemNode endNode = (MemNode) memGraph.getNodeById(endNodeId);

					Long relGID = rel.getId();
					if (rel.hasProperty(Consts.REL_GID))
						relGID = (Long) rel.getProperty(Consts.REL_GID);

					memNode.setNextRelId(relGID);
					MemRel memRel = (MemRel) memNode.createRelationshipTo(
							endNode, Consts.RelationshipTypes.DEFAULT);
					memRel.setProperty(Consts.WEIGHT, weight);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		System.out.printf("\tNode Count = %d\n", nodeCount);
		System.out.printf("\tEdge Count = %d\n", edgeCount);
		System.out.printf("\tMin Edge Weight = %f\n", normalizedMinWeight);
		System.out.printf("\tMax Edge Weight = %f\n", normalizedMaxWeight);

		return memGraph;

	}

	public static MemGraph readMemGraph(GraphDatabaseService transNeo) {
		return readMemGraph(transNeo, new HashSet<String>(),
				new HashSet<String>());
	}

	public static MemGraph readMemGraph(GraphDatabaseService transNeo,
			Set<String> nodeProps, Set<String> relProps) {

		Set<String> defaultNodeProps = new HashSet<String>(Arrays.asList(
				Consts.NODE_GID, Consts.NODE_LID, Consts.COLOR));

		Set<String> defaultRelProps = new HashSet<String>(Arrays.asList(
				Consts.REL_GID, Consts.WEIGHT));

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Loading Neo4j into MemGraph...");

		MemGraph memGraph = new MemGraph();

		Transaction tx = transNeo.beginTx();

		long nodeCount = 0;
		long edgeCount = 0;
		double minWeight = Double.MAX_VALUE;
		double maxWeight = Double.MIN_VALUE;
		double normalizedMinWeight = Consts.MIN_EDGE_WEIGHT;
		double normalizedMaxWeight = Double.MIN_VALUE;

		try {
			for (Node node : transNeo.getAllNodes()) {

				Long nodeGID = (Long) node.getProperty(Consts.NODE_GID);

				memGraph.setNextNodeId(nodeGID);
				MemNode memNode = (MemNode) memGraph.createNode();
				memNode.setProperty(Consts.NODE_GID, nodeGID);

				Byte nodeColor = -1;
				if (node.hasProperty(Consts.COLOR))
					nodeColor = (Byte) node.getProperty(Consts.COLOR);
				memNode.setProperty(Consts.COLOR, nodeColor);

				for (String key : node.getPropertyKeys()) {
					if (defaultNodeProps.contains(key))
						continue;

					if (nodeProps.contains(key))
						memNode.setProperty(key, node.getProperty(key));
				}

				nodeCount++;

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {

					edgeCount++;

					if (rel.hasProperty(Consts.WEIGHT)) {
						double weight = (Double) rel.getProperty(Consts.WEIGHT);
						if (weight > maxWeight)
							maxWeight = weight;
						if (weight < minWeight)
							minWeight = weight;
					}

				}

			}

			normalizedMinWeight = minWeight / maxWeight;
			if (normalizedMinWeight < Consts.MIN_EDGE_WEIGHT)
				normalizedMinWeight = Consts.MIN_EDGE_WEIGHT;

			for (Node node : transNeo.getAllNodes()) {

				Long nodeGID = (Long) node.getProperty(Consts.NODE_GID);

				MemNode memNode = (MemNode) memGraph.getNodeById(nodeGID);

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {

					Long endNodeId = (Long) rel.getEndNode().getProperty(
							Consts.NODE_GID);

					MemNode endNode = (MemNode) memGraph.getNodeById(endNodeId);

					Long relGID = rel.getId();
					if (rel.hasProperty(Consts.REL_GID))
						relGID = (Long) rel.getProperty(Consts.REL_GID);

					memNode.setNextRelId(relGID);
					MemRel memRel = (MemRel) memNode.createRelationshipTo(
							endNode, Consts.RelationshipTypes.DEFAULT);

					// Store normalized edge weight, [0,1]
					double weight = normalizedMinWeight;
					if (rel.hasProperty(Consts.WEIGHT)) {
						weight = (Double) rel.getProperty(Consts.WEIGHT)
								/ maxWeight;

						if (weight > normalizedMaxWeight)
							normalizedMaxWeight = weight;
					}

					memRel.setProperty(Consts.WEIGHT, weight);

					for (String key : rel.getPropertyKeys()) {
						if (defaultRelProps.contains(key))
							continue;

						if (relProps.contains(key))
							memRel.setProperty(key, rel.getProperty(key));
					}

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		System.out.printf("\tNode Count = %d\n", nodeCount);
		System.out.printf("\tEdge Count = %d\n", edgeCount);
		System.out.printf("\tMin Edge Weight = %f\n", normalizedMinWeight);
		System.out.printf("\tMax Edge Weight = %f\n", normalizedMaxWeight);

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

		if (isSupportedGraphDatabaseService(transNeo) == false)
			throw new UnsupportedOperationException(
					"GraphDatabaseService implementation not supported");

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

		Long fromId = null;
		Long toId = null;

		try {

			for (NodeData nodeAndRels : nodes) {
				fromId = (Long) nodeAndRels.getProperties()
						.get(Consts.NODE_GID);
				Node fromNode = transIndexService.getSingleNode(
						Consts.NODE_GID, fromId);
				Byte fromColor = (Byte) fromNode.getProperty(Consts.COLOR);

				for (Map<String, Object> rel : nodeAndRels.getRelationships()) {
					toId = (Long) rel.get(Consts.NODE_GID);

					Node toNode = transIndexService.getSingleNode(
							Consts.NODE_GID, toId);
					Byte toColor = (Byte) toNode.getProperty(Consts.COLOR);

					Relationship neoRel = null;

					if (fromColor == toColor) {
						neoRel = fromNode.createRelationshipTo(toNode,
								Consts.RelationshipTypes.INTERNAL);
					} else {
						neoRel = fromNode.createRelationshipTo(toNode,
								Consts.RelationshipTypes.EXTERNAL);
					}

					Long relGID = neoRel.getId();

					for (Entry<String, Object> relProp : rel.entrySet()) {

						String relPropKey = relProp.getKey();

						if (relPropKey.equals(Consts.NODE_GID))
							continue;

						if (relPropKey.equals(Consts.REL_GID)) {
							relGID = (Long) relProp.getValue();
							continue;
						}

						neoRel.setProperty(relPropKey, relProp.getValue());

					}

					neoRel.setProperty(Consts.REL_GID, relGID);

				}
			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.printf("%nfromId[%d] toId[%d]%n", fromId, toId);
		} finally {
			tx.finish();
		}
	}

	private static void applyNodeProps(IndexService transIndexService,
			GraphDatabaseService transNeo, ArrayList<NodeData> nodes) {
		Transaction tx = transNeo.beginTx();

		try {

			for (NodeData nodeAndRels : nodes) {
				Long nodeId = (Long) nodeAndRels.getProperties().get(
						Consts.NODE_LID);
				Node node = transNeo.getNodeById(nodeId);

				for (Entry<String, Object> prop : nodeAndRels.getProperties()
						.entrySet()) {

					if (prop.getKey().equals(Consts.NODE_LID))
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