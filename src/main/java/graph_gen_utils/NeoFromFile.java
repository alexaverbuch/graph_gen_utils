package graph_gen_utils;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.Consts;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.memory_graph.MemRel;
import graph_gen_utils.metrics.MetricsWriterUnweighted;
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
import org.neo4j.index.lucene.LuceneIndexBatchInserter;
import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

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

	private String databaseDir;
	private BatchInserter batchNeo = null;
	private LuceneIndexBatchInserter batchIndexService = null;
	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	public static void main(String[] args) throws Exception {

		NeoFromFile neoFromFile = new NeoFromFile("var/romania");
		Partitioner partitioner = new PartitionerAsSingle((byte) -1);
		neoFromFile.applyPtnToNeo(partitioner);
		neoFromFile.writeGMLFull("var/romania.gml");
		neoFromFile.writeChaco("var/romania.graph", ChacoType.UNWEIGHTED);

	}

	// **************
	// PUBLIC METHODS
	// **************

	public NeoFromFile(String databaseDir) {
		this.databaseDir = databaseDir;
	}

	/**
	 * Allocates nodes of a Neo4j instance to clusters/partitions. Allocation
	 * scheme is defined by the {@link Partitioner} parameter.
	 * 
	 * Method writes {@link Consts#COLOR} property to all nodes of an existing
	 * Neo4j instance. {@link Consts#NAME} property is also written to all nodes
	 * and set to {@link Node#getId()}.
	 * 
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public void applyPtnToNeo(Partitioner partitioner) throws Exception {

		openTransServices();

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
					// System.out.printf(".");
				}

				nodeNumber++;

				NodeData nodeData = new NodeData();

				nodeData.getProperties().put(Consts.ID, node.getId());
				nodeData.getProperties()
						.put(Consts.NAME, nodeNumber.toString());

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

		for (ArrayList<NodeData> nodesBuff : nodesCollection) {
			nodesBuff = partitioner.applyPartitioning(nodesBuff);
			applyNodeProps(nodesBuff);
			// System.out.printf(".");
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		closeTransServices();

	}

	/**
	 * Creates a Neo4j instance and populates it according to a generated graph
	 * topology. Examples of possible topologies are random (
	 * {@link GraphTopologyRandom}) and fully connected (
	 * {@link GraphTopologyFullyConnected}) graphs.
	 * 
	 * @param topology
	 *            instance of {@link GraphTopology} the defines generated
	 *            topology
	 */
	public void writeNeoFromTopology(GraphTopology topology) throws Exception {

		Partitioner partitioner = new PartitionerAsSingle((byte) -1);

		storePartitionedNodesAndRelsToNeo(topology, partitioner);

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
	 * @param topology
	 *            instance of {@link GraphTopology} the defines generated
	 *            topology
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public void writeNeoFromTopologyAndPtn(GraphTopology topology,
			Partitioner partitioner) throws Exception {

		storePartitionedNodesAndRelsToNeo(topology, partitioner);

	}

	/**
	 * Creates a Neo4j instance and populates it from the contents of a Chaco
	 * (.graph) file. Chaco files are basically persistent adjacency lists.
	 * 
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 */
	public void writeNeoFromChaco(String graphPath) throws Exception {

		GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
		Partitioner partitioner = new PartitionerAsSingle((byte) -1);

		storePartitionedNodesAndRelsToNeo(parser, partitioner);

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
	 * {@link NeoFromFile#writeNeoFromChacoAndPtn(String, Partitioner)} can
	 * achieve the same thing.
	 * 
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 * @param ptnPath
	 *            {@link String} representing path to .ptn file
	 */
	public void writeNeoFromChacoAndPtn(String graphPath, String ptnPath)
			throws Exception {

		GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);
		Partitioner partitioner = new PartitionerAsFile(new File(ptnPath));

		storePartitionedNodesAndRelsToNeo(parser, partitioner);

	}

	/**
	 * Creates a Neo4j instance, populates it from the contents of a Chaco
	 * (.graph) file, then allocates {@link Node}s to partitions/clusters.
	 * 
	 * Chaco files are basically persistent adjacency lists.
	 * 
	 * Allocation scheme is defined by the {@link Partitioner} parameter.
	 * 
	 * @param graphPath
	 *            {@link String} representing path to .graph file
	 * @param partitioner
	 *            implementation of {@link Partitioner} that defines
	 *            cluster/partition allocation scheme
	 */
	public void writeNeoFromChacoAndPtn(String graphPath,
			Partitioner partitioner) throws Exception {

		GraphReader parser = ChacoParserFactory.getChacoParser(graphPath);

		storePartitionedNodesAndRelsToNeo(parser, partitioner);

	}

	/**
	 * Creates a Neo4j instance and populates it from the contents of a GML
	 * (.gml) file.
	 * 
	 * GML files are basically an ASCII version of the GraphML format.
	 * 
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 */
	public void writeNeoFromGML(String gmlPath) throws Exception {

		GraphReader parser = new GMLParserDirected(new File(gmlPath));
		Partitioner partitioner = new PartitionerAsSingle((byte) -1);

		storePartitionedNodesAndRelsToNeo(parser, partitioner);

	}

	/**
	 * Creates a Chaco file and populates it with the adjacency list
	 * representation of the current Neo4j instance.
	 * 
	 * Chaco files are assumed to be undirected, this means edges are duplicated
	 * in each direction.
	 * 
	 * @param chacoPath
	 *            {@link String} representing path to .graph file
	 * @param chacoType
	 *            {@link ChacoType} specifies whether node and/or edge weights
	 *            are written to the chaco file
	 */
	public void writeChaco(String chacoPath, ChacoType chacoType)
			throws Exception {

		File chacoFile = new File(chacoPath);

		GraphWriter chacoWriter = ChacoWriterFactory.getChacoWriter(chacoType,
				chacoFile);

		writeGraphToFile(chacoWriter);

	}

	/**
	 * Creates a Chaco (.graph) file and partition (.ptn) files, populates them
	 * with the representation of the current Neo4j instance.
	 * 
	 * Chaco files are assumed to be undirected, this means edges are duplicated
	 * in each direction.
	 * 
	 * @param chacoPath
	 *            {@link String} representing path to .graph file
	 * @param chacoType
	 *            {@link ChacoType} specifies whether node and/or edge weights
	 *            are written to the chaco file
	 * @param ptnPath
	 *            {@link String} representing path to .ptn file
	 */
	public void writeChacoAndPtn(String chacoPath, ChacoType chacoType,
			String ptnPath) throws Exception {

		File chacoFile = new File(chacoPath);
		File ptnFile = new File(ptnPath);

		GraphWriter chacoPtnWriter = ChacoPtnWriterFactory.getChacoPtnWriter(
				chacoType, chacoFile, ptnFile);

		writeGraphToFile(chacoPtnWriter);

	}

	/**
	 * Creates a GML (.gml) file and populates it with the representation of the
	 * current Neo4j instance.
	 * 
	 * All {@link Node} and {@link Relationship} properties are written to the
	 * GML file.
	 * 
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 */
	public void writeGMLFull(String gmlPath) throws Exception {

		File gmlFile = new File(gmlPath);

		GraphWriter gmlWriterFull = new GMLWriterUndirectedFull(gmlFile);

		writeGraphToFile(gmlWriterFull);

	}

	/**
	 * Creates a GML (.gml) file and populates it with the representation of the
	 * current Neo4j instance.
	 * 
	 * Only certain {@link Node} and {@link Relationship} properties are written
	 * to the GML file. {@link Consts#COLOR}, {@link Consts#WEIGHT},
	 * {@link Consts#NAME}, and {@link Consts#ID}.
	 * 
	 * @param gmlPath
	 *            {@link String} representing path to .gml file
	 */
	public void writeGMLBasic(String gmlPath) throws Exception {

		File gmlFile = new File(gmlPath);

		GraphWriter gmlWriterBasic = new GMLWriterUndirectedBasic(gmlFile);

		writeGraphToFile(gmlWriterBasic);

	}

	/**
	 * Calculates graph metrics for the current Neo4j instance, creates a
	 * metrics (.met) file and populates it.
	 * 
	 * @param metricsPath
	 *            {@link String} representing path to .met file
	 */
	public void writeMetrics(String metricsPath) {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Metrics File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUnweighted.writeMetrics(transNeo, metricsFile);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		closeTransServices();

	}

	/**
	 * Calculates graph metrics for the current Neo4j instance, creates a comma
	 * separated metrics (.met) file and populates it.
	 * 
	 * @param metricsPath
	 *            {@link String} representing path to .met file
	 */
	public void writeMetricsCSV(String metricsPath) {
		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Metrics CSV File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUnweighted.writeMetricsCSV(transNeo, metricsFile, null);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		closeTransServices();
	}

	/**
	 * Calculates graph metrics for the current Neo4j instance and appends the
	 * results to a comma separated metrics (.met) file.
	 * 
	 * @param metricsPath
	 *            {@link String} representing path to .met file
	 */
	public void appendMetricsCSV(String metricsPath, Long timeStep) {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Appending Metrics CSV File...");

		File metricsFile = new File(metricsPath);
		MetricsWriterUnweighted.appendMetricsCSV(transNeo, metricsFile,
				timeStep);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		closeTransServices();

	}

	/**
	 * Loads the current Neo4j instance into a directed in-memory graph. Edges
	 * are unidirectional, and directions are exactly as in Neo4j instance.
	 * 
	 * @return {@link MemGraph}
	 */
	public MemGraph readMemGraphDirected() {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Loading Neo4j into MemGraph...");

		MemGraph memGraph = new MemGraph();

		Transaction tx = transNeo.beginTx();

		try {
			for (Node node : this.transNeo.getAllNodes()) {

				Long nodeId = Long.parseLong((String) node
						.getProperty(Consts.NAME));

				memGraph.addNode(nodeId, (Byte) node.getProperty(Consts.COLOR));

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {

					Long endNodeId = Long.parseLong((String) rel.getEndNode()
							.getProperty(Consts.NAME));

					MemRel memRel = new MemRel(endNodeId, (Double) rel
							.getProperty(Consts.WEIGHT));

					memGraph.getNode(nodeId).addNeighbour(memRel);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		closeTransServices();

		return memGraph;
	}

	/**
	 * Loads the current Neo4j instance into an undirected in-memory graph.
	 * Edges are bidirectional. Because the in-memory graph is stored as an
	 * adjacency list it means edges will be duplicated and memory consumption
	 * will be higher when using an undirected graph.
	 * 
	 * @return {@link MemGraph}
	 */
	public MemGraph readMemGraphUndirected() {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Loading Neo4j into MemGraph...");

		MemGraph memGraph = new MemGraph();

		Transaction tx = transNeo.beginTx();

		try {
			for (Node node : this.transNeo.getAllNodes()) {

				Long nodeId = Long.parseLong((String) node
						.getProperty(Consts.NAME));

				memGraph.addNode(nodeId, (Byte) node.getProperty(Consts.COLOR));

				// TODO Test
				for (Relationship rel : node.getRelationships(Direction.BOTH)) {

					double weight = 1.0;
					if (rel.hasProperty(Consts.WEIGHT))
						weight = (Double) rel.getProperty(Consts.WEIGHT);

					Long endNodeId = Long.parseLong((String) rel.getOtherNode(
							node).getProperty(Consts.NAME));

					MemRel memRel = new MemRel(endNodeId, weight);

					memGraph.getNode(nodeId).addNeighbour(memRel);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		closeTransServices();

		return memGraph;
	}

	// **************
	// PRIVATE METHODS
	// ***************

	private void writeGraphToFile(GraphWriter graphWriter) throws Exception {

		openTransServices();

		// PRINTOUT
		long time = System.currentTimeMillis();
		System.out.printf("Writing Neo4j to File...");

		graphWriter.write(transNeo);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		closeTransServices();

	}

	private void storePartitionedNodesAndRelsToNeo(GraphReader parser,
			Partitioner partitioner) throws Exception {

		openBatchServices();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Reading & Indexing Nodes...");

		ArrayList<NodeData> nodesAndRels = new ArrayList<NodeData>();

		for (NodeData nodeData : parser.getNodes()) {
			nodesAndRels.add(nodeData);

			if ((nodesAndRels.size() % Consts.STORE_BUF) == 0) {

				// PRINTOUT
				System.out.printf(".");

				nodesAndRels = partitioner.applyPartitioning(nodesAndRels);
				flushNodesBatch(nodesAndRels);
				nodesAndRels.clear();
			}
		}

		nodesAndRels = partitioner.applyPartitioning(nodesAndRels);
		flushNodesBatch(nodesAndRels);
		nodesAndRels.clear();

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
		time = System.currentTimeMillis();
		System.out.printf("Optimizing Index...");

		batchIndexService.optimize();

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		closeBatchServices();

		openTransServices();

		// PRINTOUT
		System.out.printf("Reading & Indexing Relationships...");

		time = System.currentTimeMillis();

		for (NodeData nodeData : parser.getRels()) {
			nodesAndRels.add(nodeData);

			if ((nodesAndRels.size() % Consts.STORE_BUF) == 0) {

				// PRINTOUT
				System.out.printf(".");

				flushRelsTrans(nodesAndRels);
				nodesAndRels.clear();
			}
		}

		flushRelsTrans(nodesAndRels);
		nodesAndRels.clear();

		removeReferenceNode();

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		closeTransServices();
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

	private void flushRelsTrans(ArrayList<NodeData> nodes) {
		Transaction tx = transNeo.beginTx();

		String fromName = null;
		String toName = null;

		try {

			for (NodeData nodeAndRels : nodes) {
				fromName = (String) nodeAndRels.getProperties()
						.get(Consts.NAME);
				Node fromNode = transIndexService.getSingleNode(Consts.NAME,
						fromName);
				Byte fromColor = (Byte) fromNode.getProperty(Consts.COLOR);

				for (Map<String, Object> rel : nodeAndRels.getRelationships()) {
					toName = (String) rel.get(Consts.NAME);

					Node toNode = transIndexService.getSingleNode(Consts.NAME,
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

						if (relPropKey.equals(Consts.NAME))
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

	private void applyNodeProps(ArrayList<NodeData> nodes) {
		Transaction tx = transNeo.beginTx();

		try {

			for (NodeData nodeAndRels : nodes) {
				Long nodeId = (Long) nodeAndRels.getProperties().get(Consts.ID);
				Node node = transNeo.getNodeById(nodeId);

				for (Entry<String, Object> prop : nodeAndRels.getProperties()
						.entrySet()) {

					if (prop.getKey().equals(Consts.ID))
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
	private void removeReferenceNode() {
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

	private void openBatchServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Batch Services...");

		batchNeo = new BatchInserterImpl(this.databaseDir, BatchInserterImpl
				.loadProperties("neo.props"));

		batchIndexService = new LuceneIndexBatchInserterImpl(batchNeo);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
	}

	private void closeBatchServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Closing Batch Services...");

		batchIndexService.shutdown();
		batchNeo.shutdown();

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
	}

	private void openTransServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Transactional Services...");

		transNeo = new EmbeddedGraphDatabase(this.databaseDir);
		transIndexService = new LuceneIndexService(transNeo);

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
	}

	private void closeTransServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Closing Transactional Services...");

		transIndexService.shutdown();
		transNeo.shutdown();

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
	}

	protected String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}

}