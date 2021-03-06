package graph_gen_utils;

import graph_gen_utils.general.ChacoType;
import graph_gen_utils.general.Consts;
import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.Consts.RelationshipTypes;
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
import graph_gen_utils.reader.twitter.TwitterParser;
import graph_gen_utils.writer.GraphWriter;
import graph_gen_utils.writer.chaco.ChacoPtnWriterFactory;
import graph_gen_utils.writer.chaco.ChacoWriterFactory;
import graph_gen_utils.writer.gml.GMLWriterUndirectedBasic;
import graph_gen_utils.writer.gml.GMLWriterUndirectedFull;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

import p_graph_service.PGraphDatabaseService;
import p_graph_service.core.PGraphDatabaseServiceImpl;
import p_graph_service.sim.PGraphDatabaseServiceSIM;

/**
 * Provides easy means of creating a Neo4j instance from various graph file
 * formats, loading a Neo4j instance into an in-memory graph, calculating
 * various graph metrics and writing them to file.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class NeoFromFile {

	public static void main(String[] args) throws Exception {

	}

	// **************
	// PUBLIC METHODS
	// **************

	/**
	 * Martin's code, moved from neo4j_partitioned_api. Takes a normal Neo4j
	 * instance {@link GraphDatabaseService} as input, creates a new partitioned
	 * version {@link PGraphDatabaseService} in the specified directory, then
	 * copies all data from the input instance into the new instance.
	 * {@link Node}s must have a {@link Consts#COLOR} attribute as this is used
	 * to decide which partition each {@link Node} is stored in.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing the regular Neo4j
	 *            instance
	 * @param pdbPath
	 *            {@link String} specifying the directory where partitioned
	 *            Neo4j instance should be created
	 * @return {@link PGraphDatabaseService}
	 */
	public static PGraphDatabaseService writePNeoFromNeo(String pdbPath,
			GraphDatabaseService transNeo) {

		System.out.println("Converting Neo4j to PNeo4j");

		// PGraphDatabaseService partitionedTransNeo = new
		// PGraphDatabaseServiceImpl(
		// pdbPath, 0);
		PGraphDatabaseService partitionedTransNeo = new PGraphDatabaseServiceSIM(
				pdbPath, 0);

		ArrayList<Long> nodeIDs = new ArrayList<Long>();

		// load all instance ids
		HashSet<Long> instIDs = new HashSet<Long>();
		for (Long instID : partitionedTransNeo.getInstancesIDs()) {
			instIDs.add(instID);
		}

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("\tCounting Nodes...");

		// counts all nodes
		Transaction tx = transNeo.beginTx();
		try {
			for (Node n : transNeo.getAllNodes()) {
				// ignore reference node
				if (n.getId() == 0)
					continue;

				nodeIDs.add(n.getId());
			}
			tx.success();
		} finally {
			tx.finish();
		}

		int nodesInSystem = nodeIDs.size();
		int nodeCount;
		int stepSize = 1000;
		int stepCount;

		Iterator<Long> idIter;

		// PRINTOUT
		System.out.printf("[%d]...%s", nodesInSystem, getTimeStr(System
				.currentTimeMillis()
				- time));

		nodeCount = 0;
		stepCount = 0;
		idIter = nodeIDs.iterator();
		while (idIter.hasNext()) {
			tx = transNeo.beginTx();
			try {

				// PRINTOUT
				time = System.currentTimeMillis();
				System.out.printf("\tCreating Nodes...");

				// my own transaction
				Transaction pTx = partitionedTransNeo.beginTx();
				try {
					while (idIter.hasNext() && stepCount < stepSize) {
						Node n = transNeo.getNodeById(idIter.next());

						long targetInst = (Byte) n.getProperty(Consts.COLOR);
						long gid = n.getId();

						// create instance if not yet existing
						if (!instIDs.contains(targetInst)) {
							partitionedTransNeo.addInstance(targetInst);
							instIDs.add(targetInst);
						}
						Node newN = partitionedTransNeo.createNodeOn(gid,
								targetInst);

						for (String key : n.getPropertyKeys()) {
							newN.setProperty(key, n.getProperty(key));
						}

						stepCount++;
						nodeCount++;
					}
					stepCount = 0;

					// PRINTOUT
					System.out.printf("[%d/%d]...%s", nodeCount, nodesInSystem,
							getTimeStr(System.currentTimeMillis() - time));

					pTx.success();
				} finally {
					pTx.finish();
				}
				tx.success();
			} finally {
				tx.finish();
			}
		}

		nodeCount = 0;
		stepCount = 0;
		idIter = nodeIDs.iterator();
		while (idIter.hasNext()) {
			tx = transNeo.beginTx();
			try {

				// PRINTOUT
				time = System.currentTimeMillis();
				System.out.printf("\tCreating Relationships on Nodes...");

				// my own transaction
				Transaction pTx = partitionedTransNeo.beginTx();
				try {
					while (idIter.hasNext() && stepCount < stepSize) {
						Node n = transNeo.getNodeById(idIter.next());

						long curN = n.getId();
						Node srtNode = partitionedTransNeo.getNodeById(curN);

						for (Relationship rs : n
								.getRelationships(Direction.OUTGOING)) {

							long endNodeGID = rs.getEndNode().getId();
							Node endNode = partitionedTransNeo
									.getNodeById(endNodeGID);
							Relationship newRs = srtNode.createRelationshipTo(
									endNode, rs.getType());

							// copy all properties
							for (String key : rs.getPropertyKeys()) {
								newRs.setProperty(key, rs.getProperty(key));
							}
						}
						stepCount++;
						nodeCount++;
					}
					stepCount = 0;

					// PRINTOUT
					System.out.printf("[%d/%d]...%s", nodeCount, nodesInSystem,
							getTimeStr(System.currentTimeMillis() - time));

					pTx.success();
				} finally {
					pTx.finish();
				}
				tx.success();
			} finally {
				tx.finish();
			}
		}
		transNeo.shutdown();

		return partitionedTransNeo;
	}

	/**
	 * Allocates nodes of a Neo4j instance to clusters/partitions. Allocation
	 * scheme is defined by the {@link Partitioner} parameter. Method writes
	 * {@link Consts#COLOR} property to all nodes of an existing Neo4j instance.
	 * {@link Consts#NODE_LID}, {@link Consts#NODE_GID}, {@link Consts#LATITUDE}
	 * , {@link Consts#LONGITUDE} properties are also written to all nodes.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public static void applyPtnToNeo(GraphDatabaseService transNeo,
			Partitioner partitioner) {
		applyPtnToNeo(transNeo, partitioner, new HashMap<String, Object>());
	}

	/**
	 * Allocates nodes of a Neo4j instance to clusters/partitions. Allocation
	 * scheme is defined by the {@link Partitioner} parameter. Method writes
	 * {@link Consts#COLOR} property to all nodes of an existing Neo4j instance.
	 * {@link Consts#NODE_GID} property is also written to all {@link Node}s.
	 * User may also specify additional properties (e.g. {@link Consts#LATITUDE}
	 * ) and default values. These will be written to all {@link Node}s.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 * @param props
	 *            Additional properties
	 */
	public static void applyPtnToNeo(GraphDatabaseService transNeo,
			Partitioner partitioner, Map<String, Object> props) {

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Partitioning Neo4j instance...");

		ArrayList<NodeData> nodes = new ArrayList<NodeData>();

		IndexService transIndexService = new LuceneIndexService(transNeo);

		Transaction tx = transNeo.beginTx();

		try {

			Long nodeNumber = new Long(0);

			for (Node node : transNeo.getAllNodes()) {

				if (nodes.size() % Consts.STORE_BUF == 0) {
					nodes = partitioner.applyPartitioning(nodes);
					tx.finish();

					applyNodeProps(transIndexService, transNeo, nodes);

					tx = transNeo.beginTx();
					nodes = new ArrayList<NodeData>();
				}

				NodeData nodeData = new NodeData();

				for (Entry<String, Object> prop : props.entrySet()) {
					Object val = node.getProperty(prop.getKey(), prop
							.getValue());
					nodeData.getProperties().put(prop.getKey(), val);
				}

				nodeData.getProperties().put(Consts.NODE_LID, node.getId());
				nodeData.getProperties().put(Consts.NODE_GID, ++nodeNumber);

				nodes.add(nodeData);

			}

			nodes = partitioner.applyPartitioning(nodes);
			tx.finish();

			applyNodeProps(transIndexService, transNeo, nodes);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		transIndexService.shutdown();

	}

	/**
	 * Creates a Neo4j instance and populates it according to a generated graph
	 * topology. Examples of possible topologies are random (
	 * {@link GraphTopologyRandom}) and fully connected (
	 * {@link GraphTopologyFullyConnected}) graphs.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param topology
	 *            instance of {@link GraphTopology} the defines generated
	 *            topology
	 */
	public static void writeNeoFromTopology(GraphDatabaseService transNeo,
			GraphTopology topology) {

		try {
			Partitioner partitioner = new PartitionerAsSingle((byte) -1);
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createTransactionalWrapper(transNeo);
			storePartitionedNodesAndRelsToNeo(dbWrapper, topology, partitioner);
			dbWrapper.shutdownIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Neo4j instance, populates it according to a generated graph
	 * topology, then allocates {@link Node}s to partitions/clusters. Examples
	 * of possible topologies are random ({@link GraphTopologyRandom}) and fully
	 * connected ({@link GraphTopologyFullyConnected}) graphs. Allocation scheme
	 * is defined by the {@link Partitioner} parameter.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param topology
	 *            instance of {@link GraphTopology} the defines generated
	 *            topology
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public static void writeNeoFromTopologyAndPtn(
			GraphDatabaseService transNeo, GraphTopology topology,
			Partitioner partitioner) {

		try {
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createTransactionalWrapper(transNeo);
			storePartitionedNodesAndRelsToNeo(dbWrapper, topology, partitioner);
			dbWrapper.shutdownIndex();
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
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 */
	public static void writeNeoFromChaco(GraphDatabaseService transNeo,
			String graphPath) {

		try {
			GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
			Partitioner partitioner = new PartitionerAsSingle((byte) -1);
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createTransactionalWrapper(transNeo);
			storePartitionedNodesAndRelsToNeo(dbWrapper, parser, partitioner);
			dbWrapper.shutdownIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Neo4j instance, populates it from the contents of a Chaco
	 * (.graph) file, then allocates {@link Node}s to partitions/clusters. Chaco
	 * files are basically persistent adjacency lists. Partition/cluster
	 * allocation is defined by the contents of a .ptn file. This method is only
	 * included for convenience/ease of use.
	 * {@link NeoFromFile#writeNeoFromChacoAndPtn(GraphDatabaseService, String, Partitioner)}
	 * can achieve the same thing.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 * @param ptnPath
	 *            {@link String} representing path to .ptn file
	 */
	public static void writeNeoFromChacoAndPtn(GraphDatabaseService transNeo,
			String graphPath, String ptnPath) {

		try {
			GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
			Partitioner partitioner = new PartitionerAsFile(new File(ptnPath));
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createTransactionalWrapper(transNeo);
			storePartitionedNodesAndRelsToNeo(dbWrapper, parser, partitioner);
			dbWrapper.shutdownIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Neo4j instance, populates it from the contents of a Chaco
	 * (.graph) file, then allocates {@link Node}s to partitions/clusters. Chaco
	 * files are basically persistent adjacency lists. Allocation scheme is
	 * defined by the {@link Partitioner} parameter.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public static void writeNeoFromChacoAndPtn(GraphDatabaseService transNeo,
			String graphPath, Partitioner partitioner) {

		try {
			GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createTransactionalWrapper(transNeo);
			storePartitionedNodesAndRelsToNeo(dbWrapper, parser, partitioner);
			dbWrapper.shutdownIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Neo4j instance using the {@link BatchInserter}, populates it
	 * from the contents of a Chaco (.graph) file, then allocates {@link Node}s
	 * to partitions/clusters. Chaco files are basically persistent adjacency
	 * lists. Allocation scheme is defined by the {@link Partitioner} parameter.
	 * 
	 * @param dbDir
	 *            {@link String} representing the path to a Neo4j instance
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public static void writeNeoFromChacoAndPtnBatch(String dbDir,
			String graphPath, Partitioner partitioner) {

		try {
			GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createBatchWrapper(dbDir);
			storePartitionedNodesAndRelsToNeo(dbWrapper, parser, partitioner);
			dbWrapper.shutdownDbAndIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Neo4j instance and populates it from the contents of a GML
	 * (.gml) file. GML files are basically an ASCII version of the GraphML
	 * format.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 */
	public static void writeNeoFromGML(GraphDatabaseService transNeo,
			String gmlPath) {

		try {
			GraphReader parser = new GMLParserDirected(new File(gmlPath));
			Partitioner partitioner = new PartitionerAsSingle((byte) -1);
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createTransactionalWrapper(transNeo);
			storePartitionedNodesAndRelsToNeo(dbWrapper, parser, partitioner);
			dbWrapper.shutdownIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Neo4j instance and populates it from the contents of a GML
	 * (.gml) file, then allocates {@link Node}s to partitions/clusters. GML
	 * files are basically an ASCII version of the GraphML format. Allocation
	 * scheme is defined by the {@link Partitioner} parameter.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public static void writeNeoFromGMLAndPtn(GraphDatabaseService transNeo,
			String gmlPath, Partitioner partitioner) {

		try {
			GraphReader parser = new GMLParserDirected(new File(gmlPath));
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createTransactionalWrapper(transNeo);
			storePartitionedNodesAndRelsToNeo(dbWrapper, parser, partitioner);
			dbWrapper.shutdownIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Neo4j instance and populates it from the contents of a dataset
	 * with a proprietry binary file format, which contains user
	 * follows/following connectivity data from Twitter. The file was obtained
	 * by crawling Twitter for 300 hours.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param twitterPath
	 *            {@link String} representing path to Twitter dataset
	 */
	public static void writeNeoFromTwitterDataset(
			GraphDatabaseService transNeo, String twitterPath) {

		try {
			GraphReader parser = new TwitterParser(new File(twitterPath));
			Partitioner partitioner = new PartitionerAsSingle((byte) -1);
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createTransactionalWrapper(transNeo);
			storePartitionedNodesAndRelsToNeo(dbWrapper, parser, partitioner);
			dbWrapper.shutdownIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Neo4j instance and populates it from the contents of a dataset
	 * with a proprietry binary file format, which contains user
	 * follows/following connectivity data from Twitter. The file was obtained
	 * by crawling Twitter for 300 hours. Allocates {@link Node}s to
	 * partitions/clusters. Allocation scheme is defined by the
	 * {@link Partitioner} parameter.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param twitterPath
	 *            {@link String} representing path to Twitter dataset
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public static void writeNeoFromTwitterDatasetAndPtn(
			GraphDatabaseService transNeo, String twitterPath,
			Partitioner partitioner) {

		try {
			GraphReader parser = new TwitterParser(new File(twitterPath));
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createTransactionalWrapper(transNeo);
			storePartitionedNodesAndRelsToNeo(dbWrapper, parser, partitioner);
			dbWrapper.shutdownIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Neo4j instance using the {@link BatchInserter}, then populates
	 * it from the contents of a dataset with a proprietry binary file format,
	 * which contains user follows/following connectivity data from Twitter. The
	 * file was obtained by crawling Twitter for 300 hours.
	 * 
	 * @param dbDir
	 *            {@link String} representing the path to a Neo4j instance
	 * @param twitterPath
	 *            {@link String} representing path to Twitter dataset
	 */
	public static void writeNeoFromTwitterDatasetBatch(String dbDir,
			String twitterPath) {

		try {
			GraphReader parser = new TwitterParser(new File(twitterPath));
			Partitioner partitioner = new PartitionerAsSingle((byte) -1);
			GraphDatabaseServicesWriter dbWrapper = GraphDatabaseServicesWriter
					.createBatchWrapper(dbDir);
			storePartitionedNodesAndRelsToNeo(dbWrapper, parser, partitioner);
			dbWrapper.shutdownDbAndIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Creates a Chaco file and populates it with the adjacency list
	 * representation of the current Neo4j instance. Chaco files are assumed to
	 * be undirected, this means edges are duplicated in each direction.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param chacoPath
	 *            {@link String} representing path to .graph file
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
	 * with the representation of the current Neo4j instance. Chaco files are
	 * assumed to be undirected, this means edges are duplicated in each
	 * direction.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param chacoPath
	 *            {@link String} representing path to .graph file
	 * @param chacoType
	 *            {@link ChacoType} specifies whether node and/or edge weights
	 *            are written to the chaco file
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
	 * current Neo4j instance. All {@link Node} and {@link Relationship}
	 * properties are written to the GML file.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
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
	 * current Neo4j instance. Only certain {@link Node} and
	 * {@link Relationship} properties are written to the GML file.
	 * {@link Consts#COLOR}, {@link Consts#WEIGHT}, {@link Consts#NODE_GID}.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
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
	 * @param metricsPath
	 *            {@link String} representing path to .met file
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
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @return {@link MemGraph}
	 */
	public static MemGraph readMemGraph(GraphDatabaseService transNeo) {
		return readMemGraph(transNeo, new HashSet<String>(),
				new HashSet<String>());
	}

	public static MemGraph readMemGraph(GraphDatabaseService transNeo,
			Set<String> nodeProps, Set<String> relProps) {

		Set<String> ignoreNodeProps = new HashSet<String>(Arrays.asList(
				Consts.NODE_GID, Consts.NODE_LID, Consts.COLOR));

		Set<String> ignoreRelProps = new HashSet<String>(Arrays.asList(
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

				memGraph.setNextNodeId(node.getId());
				MemNode memNode = (MemNode) memGraph.createNode();

				// FIXME UNCOMMENT (Performance)
				memNode.setProperty(Consts.NODE_GID, (Long) node
						.getProperty(Consts.NODE_GID));

				Byte nodeColor = -1;
				if (node.hasProperty(Consts.COLOR))
					nodeColor = (Byte) node.getProperty(Consts.COLOR);
				memNode.setProperty(Consts.COLOR, nodeColor);

				// FIXME UNCOMMENT (Performance)
				// for (String key : node.getPropertyKeys()) {
				// if (ignoreNodeProps.contains(key))
				// continue;
				//
				// if (nodeProps.contains(key))
				// memNode.setProperty(key, node.getProperty(key));
				// }

				// FIXME UNCOMMENT (Performance)
				// for (Relationship rel : node
				// .getRelationships(Direction.OUTGOING)) {
				//
				// if (rel.hasProperty(Consts.WEIGHT)) {
				// double weight = (Double) rel.getProperty(Consts.WEIGHT);
				// if (weight > maxWeight)
				// maxWeight = weight;
				// if (weight < minWeight)
				// minWeight = weight;
				// }
				//
				// }

			}

			// FIXME UNCOMMENT (Performance)
			// if ((minWeight == Double.MAX_VALUE)
			// || (maxWeight == Double.MIN_VALUE)) {
			// minWeight = 1.0;
			// maxWeight = 1.0;
			// }
			// normalizedMinWeight = minWeight / maxWeight;
			// if (normalizedMinWeight < Consts.MIN_EDGE_WEIGHT)
			// normalizedMinWeight = Consts.MIN_EDGE_WEIGHT;
			minWeight = 1.0;
			maxWeight = 1.0;
			normalizedMinWeight = 1.0;
			normalizedMaxWeight = 1.0;

			for (Node node : transNeo.getAllNodes()) {

				nodeCount++;

				MemNode memNode = (MemNode) memGraph.getNodeById(node.getId());

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {

					edgeCount++;

					MemNode endNode = (MemNode) memGraph.getNodeById(rel
							.getEndNode().getId());

					memNode.setNextRelId(rel.getId());
					MemRel memRel = (MemRel) memNode.createRelationshipTo(
							endNode, Consts.RelationshipTypes.DEFAULT);

					// FIXME UNCOMMENT (Performance)
					// Long relGID = rel.getId();
					// if (rel.hasProperty(Consts.REL_GID))
					// relGID = (Long) rel.getProperty(Consts.REL_GID);
					//
					// memRel.setProperty(Consts.REL_GID, relGID);

					// FIXME UNCOMMENT (Performance)
					// // Store normalized edge weight, [0,1]
					// double weight = normalizedMinWeight;
					//
					// if (rel.hasProperty(Consts.WEIGHT)) {
					// weight = (Double) rel.getProperty(Consts.WEIGHT)
					// / maxWeight;
					// }
					//
					// if (weight > normalizedMaxWeight)
					// normalizedMaxWeight = weight;
					//
					// memRel.setProperty(Consts.WEIGHT, weight);
					memRel.setProperty(Consts.WEIGHT, 1d);

					// FIXME UNCOMMENT (Performance)
					// for (String key : rel.getPropertyKeys()) {
					// if (ignoreRelProps.contains(key))
					// continue;
					//
					// if (relProps.contains(key))
					// memRel.setProperty(key, rel.getProperty(key));
					// }

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

	/**
	 * Deletes all {@link Node}s that do not have at least one
	 * {@link Relationship} from the Neo4j instance.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 */
	public static void removeOrphanNodes(GraphDatabaseService transNeo) {

		long time = System.currentTimeMillis();
		System.out.printf("Deleting orphan nodes...");

		long deletedNodes = 0;

		Transaction tx = transNeo.beginTx();

		try {
			for (Node node : transNeo.getAllNodes()) {

				if (node.hasRelationship() == true)
					continue;

				node.delete();

				if (++deletedNodes % Consts.STORE_BUF == 0) {
					tx.success();
					tx.finish();
					tx = transNeo.beginTx();
				}

			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("Deleted [%d]...%s", deletedNodes, getTimeStr(System
				.currentTimeMillis()
				- time));

	}

	/**
	 * Deletes all {@link Relationship}s of the specified
	 * {@link RelationshipType} values (as given by the {@link String} values in
	 * relTypes parameter) from the Neo4j instance.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param relTypes
	 *            {@link HashSet} of {@link RelationshipType} representing the
	 *            {@link Relationship}s that should be deleted
	 */
	public static void removeRelationshipsByType(GraphDatabaseService transNeo,
			HashSet<String> relTypes) {

		long time = System.currentTimeMillis();
		System.out.printf("Deleting relationships by type...");

		int deletedRels = 0;

		Transaction tx = transNeo.beginTx();

		try {
			for (Node node : transNeo.getAllNodes()) {

				if (relTypes.size() <= 0)
					break;

				for (Relationship relationship : node
						.getRelationships(Direction.OUTGOING)) {

					// Don't delete Relationships of this type
					if (relTypes.contains(relationship.getType().name()) == false)
						continue;

					relationship.delete();

					if (++deletedRels % Consts.STORE_BUF == 0) {
						tx.success();
						tx.finish();
						tx = transNeo.beginTx();
					}

				}

			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("Deleted [%d]...%s", deletedRels, getTimeStr(System
				.currentTimeMillis()
				- time));

	}

	/**
	 * Deletes duplicate {@link Relationship}s (all but one {@link Relationship}
	 * between any two {@link Node}s). Useful for reducing the size of a Neo4j
	 * instance while maintaining the same basic connectivity/structure.
	 * {@link Direction} is considered when identifying duplicates.
	 * {@link RelationshipType} is NOT considered (they are all regarded as
	 * equal).
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param direction
	 *            {@link Direction} which defines what a duplicate is. If
	 *            direction equals {@link Direction#OUTGOING} then all but one
	 *            outgoing {@link Relationship}s between any two {@link Node}s
	 *            are kept. In this case two {@link Relationship}s may exist
	 *            between a pair of {@link Node}s. If direction equals
	 *            {@link Direction#BOTH} then all but one {@link Relationship}
	 *            of any direction between any two {@link Node} s is kept.
	 */
	public static void removeDuplicateRelationships(
			GraphDatabaseService transNeo, Direction direction) {

		long time = System.currentTimeMillis();
		System.out.printf("Deleting duplicate relationships...");

		int deletedRels = 0;

		Transaction tx = transNeo.beginTx();

		try {
			for (Node node : transNeo.getAllNodes()) {

				HashSet<Long> neighbourIds = new HashSet<Long>();

				for (Relationship relationship : node
						.getRelationships(direction)) {

					Node neighbour = relationship.getOtherNode(node);

					// Neighbour has not been seen previously
					if (neighbourIds.add(neighbour.getId()) == true)
						continue;

					relationship.delete();

					if (++deletedRels % Consts.STORE_BUF == 0) {
						tx.success();
						tx.finish();
						tx = transNeo.beginTx();
					}

				}

			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("Deleted [%d]...%s", deletedRels, getTimeStr(System
				.currentTimeMillis()
				- time));

	}

	/**
	 * Deletes a specified number of {@link Relationship}s by selecting them
	 * from among all {@link Relationship} s and then deleting them from the
	 * Neo4j instance. Selection is performed uniformly random.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param percRelsToKeep
	 *            percentage of {@link Relationship} s (between the ID space
	 *            [0,maxRelId]) that should not be deleted
	 * @param maxRelId
	 *            {@link Relationship} s to delete are chosen from the ID space
	 *            between 0 and maxRelId
	 */
	public static void removeRandomRelationships(GraphDatabaseService transNeo,
			double percRelsToKeep, int maxRelId) {

		long time = System.currentTimeMillis();
		long flushTime = System.currentTimeMillis();

		System.out.println("Deleting random relationships...");

		int deletedRelsCount = 0;

		Transaction tx = transNeo.beginTx();

		try {
			Random rand = new Random();

			for (int index = 0; index < maxRelId; index++) {

				if (rand.nextDouble() < percRelsToKeep)
					continue;

				Relationship rel = null;
				try {
					rel = transNeo.getRelationshipById(index);
				} catch (Exception e) {
					// Relationship was not found
					continue;
				}

				rel.delete();

				if (++deletedRelsCount % Consts.STORE_BUF == 0) {
					tx.success();
					tx.finish();
					tx = transNeo.beginTx();

					if (deletedRelsCount % 1000000 == 0) {
						System.out.printf("\t[%d]...%s", deletedRelsCount,
								getTimeStr(System.currentTimeMillis()
										- flushTime));
						flushTime = System.currentTimeMillis();
					}
				}

			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("\tDeleted [%d]...%s", deletedRelsCount,
				getTimeStr(System.currentTimeMillis() - time));

	}

	/**
	 * Deletes a specified number of {@link Node}s by selecting them from among
	 * all {@link Node}s and then deleting them from the Neo4j instance.
	 * Selection is performed uniformly random.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} representing a Neo4j instance
	 * @param percNodesToKeep
	 *            percentage of {@link Node}s that should not be deleted
	 */
	public static void removeRandomNodes(GraphDatabaseService transNeo,
			double percNodesToKeep) {

		long time = System.currentTimeMillis();
		long flushTime = System.currentTimeMillis();

		System.out.println("Deleting random nodes...");

		int deletedRelsCount = 0;
		int deletedNodesCount = 0;
		int storeBuff = 0;

		Transaction tx = transNeo.beginTx();

		try {
			Random rand = new Random();

			for (Node node : transNeo.getAllNodes()) {

				if (rand.nextDouble() < percNodesToKeep)
					continue;

				for (Relationship rel : node.getRelationships()) {
					rel.delete();
					deletedRelsCount++;
					storeBuff++;
				}

				node.delete();
				deletedNodesCount++;
				storeBuff++;

				if (storeBuff >= Consts.STORE_BUF) {
					storeBuff = 0;

					tx.success();
					tx.finish();
					tx = transNeo.beginTx();

					// if ((deletedRelsCount + deletedNodesCount) % 1000000 ==
					// 0) {
					System.out.printf("\t[%d,%d]...%s", deletedNodesCount,
							deletedRelsCount, getTimeStr(System
									.currentTimeMillis()
									- flushTime));
					flushTime = System.currentTimeMillis();
					// }
				}

			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out
				.printf("\tDeleted [%d,%d]...%s", deletedNodesCount,
						deletedRelsCount, getTimeStr(System.currentTimeMillis()
								- time));

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
			GraphDatabaseServicesWriter dbWrapper, GraphReader parser,
			Partitioner partitioner) throws Exception {

		// FIXME Temp!!!!!!!!!!!!!!
		int readSoFar = 0;
		long timeSoFar = System.currentTimeMillis();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Reading & Indexing Nodes...");

		ArrayList<NodeData> nodesAndRels = new ArrayList<NodeData>();

		for (NodeData nodeData : parser.getNodes()) {
			nodesAndRels.add(nodeData);

			if ((nodesAndRels.size() % Consts.STORE_BUF) == 0) {

				// FIXME Temp!!!!!!!!!!!!!!
				if ((readSoFar += Consts.STORE_BUF) % 1000000 == 0) {
					System.out.printf("\nRead so far [%d]...[%s]...",
							readSoFar, getTimeStr(System.currentTimeMillis()
									- timeSoFar));
					timeSoFar = System.currentTimeMillis();
				}

				// PRINTOUT
				// System.out.printf(".");

				nodesAndRels = partitioner.applyPartitioning(nodesAndRels);
				dbWrapper.flushNodes(nodesAndRels);
				nodesAndRels.clear();
			}
		}

		nodesAndRels = partitioner.applyPartitioning(nodesAndRels);
		dbWrapper.flushNodes(nodesAndRels);
		nodesAndRels.clear();

		dbWrapper.optimizeIndex();

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
		time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Reading & Indexing Relationships...");

		// FIXME Temp!!!!!!!!!!!!!!
		readSoFar = 0;
		timeSoFar = System.currentTimeMillis();

		for (NodeData nodeData : parser.getRels()) {
			nodesAndRels.add(nodeData);

			if ((nodesAndRels.size() % Consts.STORE_BUF) == 0) {

				// FIXME Temp!!!!!!!!!!!!!!
				if ((readSoFar += Consts.STORE_BUF) % 1000000 == 0) {
					System.out.printf("\nRead so far [%d]...[%s]...",
							readSoFar, getTimeStr(System.currentTimeMillis()
									- timeSoFar));
					timeSoFar = System.currentTimeMillis();
				}

				// PRINTOUT
				// System.out.printf(".");

				dbWrapper.flushRels(nodesAndRels);
				nodesAndRels.clear();
			}
		}

		dbWrapper.flushRels(nodesAndRels);
		nodesAndRels.clear();

		dbWrapper.removeReferenceNode();

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

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

	private static String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}

}
