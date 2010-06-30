package dodgy_tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.general.ChacoType;
import graph_gen_utils.general.Consts;
import graph_gen_utils.general.Utils;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.memory_graph.MemNode;
import graph_gen_utils.partitioner.Partitioner;
import graph_gen_utils.partitioner.PartitionerAsBalanced;
import graph_gen_utils.partitioner.PartitionerAsCoordinates;
import graph_gen_utils.partitioner.PartitionerAsDefault;
import graph_gen_utils.partitioner.PartitionerAsRandom;
import graph_gen_utils.partitioner.PartitionerAsSingle;
import graph_gen_utils.partitioner.PartitionerAsCoordinates.BorderType;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import p_graph_service.PGraphDatabaseService;

public class DodgyTests {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		do_partition_GIS();
	}

	private static void create_network_traffic_graphs() {
		double[] weights;
		String dbName;

		// **************
		// *** GLOBAL ***
		// **************
		//
		// HARD 4
		// 0↔1, 2↔3, 1↔3, 1↔2, 0↔2, 0↔3
		// 0.41 0.28 0.00 1.00 0.00 0.00
		weights = new double[] { 0.41, 0.28, 0.00, 1.00, 0.00, 0.00 };
		dbName = "global_hard_4";
		make_network_traffic_graph(weights, dbName);

		// DiDiC 4
		// 0↔1, 2↔3, 1↔3, 1↔2, 0↔2, 0↔3
		// 0.98 0.98 0.97 1.00 0.97 0.98
		weights = new double[] { 0.98, 0.98, 0.97, 1.00, 0.97, 0.98 };
		dbName = "global_didic_4";
		make_network_traffic_graph(weights, dbName);

		// RAND 4
		// 0↔1, 2↔3, 1↔3, 1↔2, 0↔2, 0↔3
		// 0.99 0.99 0.99 1.00 1.00 0.99
		weights = new double[] { 0.99, 0.99, 0.99, 1.00, 1.00, 0.99 };
		dbName = "global_rand_4";
		make_network_traffic_graph(weights, dbName);

		// **************
		// *** LOCAL ****
		// **************
		// HARD 4
		// 0↔1, 2↔3, 1↔3, 1↔2, 0↔2, 0↔3
		// 1.00 1.00 1.00 1.00 1.00 1.00
		weights = new double[] { 1.00, 1.00, 1.00, 1.00, 1.00, 1.00 };
		dbName = "local_hard_4";
		make_network_traffic_graph(weights, dbName);

		// DiDiC 4
		// 0↔1, 2↔3, 1↔3, 1↔2, 0↔2, 0↔3
		// 0.63 0.67 0.69 0.59 1.00 0.53
		weights = new double[] { 0.63, 0.67, 0.69, 0.59, 1.00, 0.53 };
		dbName = "local_didic_4";
		make_network_traffic_graph(weights, dbName);

		// RAND 4
		// 0↔1, 2↔3, 1↔3, 1↔2, 0↔2, 0↔3
		// 1.00 0.94 0.95 0.96 0.97 0.97
		weights = new double[] { 1.00, 0.94, 0.95, 0.96, 0.97, 0.97 };
		dbName = "local_rand_4";
		make_network_traffic_graph(weights, dbName);
	}

	private static void make_network_traffic_graph(double[] weights,
			String dbName) {
		Double minLat = 1d, minLon = 1d;
		Double maxLat = 100d, maxLon = 100d;
		String dbDir = "var/" + dbName;
		Utils.cleanDir(dbDir);
		GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);
		Transaction tx = db.beginTx();
		try {
			Node nodePtn0 = db.createNode();
			nodePtn0.setProperty(Consts.NAME, "Partition 0");
			nodePtn0.setProperty(Consts.LONGITUDE, minLon);
			nodePtn0.setProperty(Consts.LATITUDE, minLat);

			Node nodePtn1 = db.createNode();
			nodePtn1.setProperty(Consts.NAME, "Partition 1");
			nodePtn1.setProperty(Consts.LONGITUDE, maxLon);
			nodePtn1.setProperty(Consts.LATITUDE, minLat);

			Node nodePtn2 = db.createNode();
			nodePtn2.setProperty(Consts.NAME, "Partition 2");
			nodePtn2.setProperty(Consts.LONGITUDE, maxLon);
			nodePtn2.setProperty(Consts.LATITUDE, maxLat);

			Node nodePtn3 = db.createNode();
			nodePtn3.setProperty(Consts.NAME, "Partition 3");
			nodePtn3.setProperty(Consts.LONGITUDE, minLon);
			nodePtn3.setProperty(Consts.LATITUDE, maxLat);

			Relationship rel0to1 = nodePtn0.createRelationshipTo(nodePtn1,
					Consts.RelationshipTypes.DEFAULT);
			rel0to1.setProperty(Consts.WEIGHT, weights[0]);

			Relationship rel2to3 = nodePtn2.createRelationshipTo(nodePtn3,
					Consts.RelationshipTypes.DEFAULT);
			rel2to3.setProperty(Consts.WEIGHT, weights[1]);

			Relationship rel1to3 = nodePtn1.createRelationshipTo(nodePtn3,
					Consts.RelationshipTypes.DEFAULT);
			rel1to3.setProperty(Consts.WEIGHT, weights[2]);

			Relationship rel1to2 = nodePtn1.createRelationshipTo(nodePtn2,
					Consts.RelationshipTypes.DEFAULT);
			rel1to2.setProperty(Consts.WEIGHT, weights[3]);

			Relationship rel0to2 = nodePtn0.createRelationshipTo(nodePtn2,
					Consts.RelationshipTypes.DEFAULT);
			rel0to2.setProperty(Consts.WEIGHT, weights[4]);

			Relationship rel0to3 = nodePtn0.createRelationshipTo(nodePtn3,
					Consts.RelationshipTypes.DEFAULT);
			rel0to3.setProperty(Consts.WEIGHT, weights[5]);

			db.getReferenceNode().delete();

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		NeoFromFile.applyPtnToNeo(db, new PartitionerAsBalanced((byte) 4));
		NeoFromFile.writeGMLFull(db, "var/" + dbName + ".gml");
		db.shutdown();
	}

	private static void test_transactional_vs_batch() {
		String testDir = "/media/disk/alex/Neo4j/test/";
		String dbTransDir = testDir + "transNeo/";
		String dbBatchDir = testDir + "batchNeo/";
		String testGraphDir = "/home/alex/workspace/graph_cluster_utils/graphs/";
		String testGraph = testGraphDir + "m14b.graph";

		// DirUtils.cleanDir(testDir);
		// DirUtils.cleanDir(dbTransDir);
		// DirUtils.cleanDir(dbBatchDir);
		//    
		// Partitioner partitioner = new PartitionerAsDefault();
		//    
		// long time = System.currentTimeMillis();
		// NeoFromFile
		// .writeNeoFromChacoAndPtnBatch(dbBatchDir, testGraph, partitioner);
		// System.out.printf("Neo from Chaco [Batch]...%s", getTimeStr(System
		// .currentTimeMillis()
		// - time));
		//    
		// GraphDatabaseService dbBatch = new EmbeddedGraphDatabase(dbBatchDir);
		// NeoFromFile.writeMetricsCSV(dbBatch, testDir + "batch.met");
		// dbBatch.shutdown();
		//    
		// time = System.currentTimeMillis();
		GraphDatabaseService dbTrans = new EmbeddedGraphDatabase(dbTransDir);
		// NeoFromFile.writeNeoFromChacoAndPtn(dbTrans, testGraph, partitioner);
		// System.out.printf("Neo from Chaco [Transactional]...%s",
		// getTimeStr(System
		// .currentTimeMillis()
		// - time));

		NeoFromFile.writeMetricsCSV(dbTrans, testDir + "trans.met");
		dbTrans.shutdown();

	}

	private static void do_partition_GIS() {
		String dirStr = "/home/alex/workspace/graph_cluster_utils/sample dbs/simulated - gis romania/";
		// String dbStr = dirStr
		// + "romania-gis-COORD-BAL_NS2-GID-NAME-COORDS-BICYCLE";
		String dbStr = dirStr
				+ "romania-gis-COORD-BAL_NS4-GID-NAME-COORDS-BICYCLE";

		double northWesternLon = 20d;
		double northWesternLat = 49d;
		double southEasternLon = 31d;
		double southEasternLat = 43d;

		GraphDatabaseService db = new EmbeddedGraphDatabase(dbStr);

		BorderType borderType = BorderType.NORTH_SOUTH_BORDERS;

		// double[] borders = new double[] { 25.54991 };
		double[] borders = new double[] { 23.6699606, 25.54991, 26.2199546 };

		Partitioner partitioner = new PartitionerAsCoordinates(northWesternLon,
				northWesternLat, southEasternLon, southEasternLat, borderType,
				borders);

		HashMap<String, Object> props = new HashMap<String, Object>();
		props.put(Consts.LONGITUDE, 0d);
		props.put(Consts.LATITUDE, 0d);

		NeoFromFile.applyPtnToNeo(db, partitioner, props);

		// NeoFromFile.writeMetricsCSV(db, dirStr + "gis-hard2.met");
		NeoFromFile.writeMetricsCSV(db, dirStr + "gis-hard4.met");

		db.shutdown();
	}

	private static void test_partitioner_as_coords() {
		String dbStr = "/media/disk/alex/Neo4j/test";
		String gml0Str = "/media/disk/alex/Neo4j/test.0.gml";
		String gml1Str = "/media/disk/alex/Neo4j/test.1.gml";

		Utils.cleanDir(dbStr);
		GraphDatabaseService db = new EmbeddedGraphDatabase(dbStr);

		Transaction tx = db.beginTx();

		RelationshipType relType1 = DynamicRelationshipType
				.withName("RelType1");

		try {

			Node refNode = db.getReferenceNode();
			refNode.delete();

			Node node1 = db.createNode();
			Node node2 = db.createNode();
			Node node3 = db.createNode();
			Node node4 = db.createNode();

			node1.createRelationshipTo(node2, relType1);
			node2.createRelationshipTo(node3, relType1);
			node3.createRelationshipTo(node4, relType1);
			node4.createRelationshipTo(node1, relType1);

			node1.setProperty(Consts.LATITUDE, 50d);
			node1.setProperty(Consts.LONGITUDE, -100d);
			node1.setProperty(Consts.NAME, node1.getProperty(Consts.LONGITUDE)
					.toString()
					+ " " + node1.getProperty(Consts.LATITUDE).toString());

			node2.setProperty(Consts.LATITUDE, 50d);
			node2.setProperty(Consts.LONGITUDE, 100d);
			node2.setProperty(Consts.NAME, node2.getProperty(Consts.LONGITUDE)
					.toString()
					+ " " + node2.getProperty(Consts.LATITUDE).toString());

			node3.setProperty(Consts.LATITUDE, -50d);
			node3.setProperty(Consts.LONGITUDE, 100d);
			node3.setProperty(Consts.NAME, node3.getProperty(Consts.LONGITUDE)
					.toString()
					+ " " + node3.getProperty(Consts.LATITUDE).toString());

			node4.setProperty(Consts.LATITUDE, -50d);
			node4.setProperty(Consts.LONGITUDE, -100d);
			node4.setProperty(Consts.NAME, node4.getProperty(Consts.LONGITUDE)
					.toString()
					+ " " + node4.getProperty(Consts.LATITUDE).toString());

			tx.success();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		Partitioner partitionerSingle = new PartitionerAsSingle((byte) 0);
		NeoFromFile.applyPtnToNeo(db, partitionerSingle);

		NeoFromFile.writeGMLFull(db, gml0Str);

		Partitioner partitionerCoords = new PartitionerAsCoordinates(-100, 51,
				101, -51, BorderType.NORTH_SOUTH_BORDERS, (byte) 2);
		HashMap<String, Object> props = new HashMap<String, Object>();
		props.put(Consts.LONGITUDE, 0d);
		props.put(Consts.LATITUDE, 0d);
		NeoFromFile.applyPtnToNeo(db, partitionerCoords, props);

		NeoFromFile.writeGMLFull(db, gml1Str);

		db.shutdown();
	}

	private static void test_delete_duplicate_edges() {
		String dbStr = "/media/disk/alex/Neo4j/test";
		String gml0Str = "/media/disk/alex/Neo4j/test.0.gml";
		String gml1Str = "/media/disk/alex/Neo4j/test.1.gml";
		String gml2Str = "/media/disk/alex/Neo4j/test.2.gml";

		Utils.cleanDir(dbStr);
		GraphDatabaseService db = new EmbeddedGraphDatabase(dbStr);

		Transaction tx = db.beginTx();

		RelationshipType relType1 = DynamicRelationshipType
				.withName("RelType1");

		try {

			Node refNode = db.getReferenceNode();
			refNode.delete();

			Node node1 = db.createNode();
			Node node2 = db.createNode();
			Node node3 = db.createNode();
			Node node4 = db.createNode();

			node1.createRelationshipTo(node2, relType1);
			node1.createRelationshipTo(node2, relType1);
			node2.createRelationshipTo(node1, relType1);
			node2.createRelationshipTo(node3, relType1);
			node3.createRelationshipTo(node4, relType1);
			node4.createRelationshipTo(node1, relType1);

			tx.success();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		Partitioner partitioner = new PartitionerAsSingle((byte) 0);
		NeoFromFile.applyPtnToNeo(db, partitioner);

		NeoFromFile.writeGMLBasic(db, gml0Str);

		NeoFromFile.removeDuplicateRelationships(db, Direction.BOTH);

		NeoFromFile.writeGMLBasic(db, gml1Str);

		NeoFromFile.removeDuplicateRelationships(db, Direction.OUTGOING);

		NeoFromFile.writeGMLBasic(db, gml2Str);

		db.shutdown();
	}

	private static void test_delete_edge_types_and_orphan_nodes() {
		String dbStr = "/media/disk/alex/Neo4j/test";
		String gml0Str = "/media/disk/alex/Neo4j/test.0.gml";
		String gml1Str = "/media/disk/alex/Neo4j/test.1.gml";
		String gml2Str = "/media/disk/alex/Neo4j/test.2.gml";

		Utils.cleanDir(dbStr);
		GraphDatabaseService db = new EmbeddedGraphDatabase(dbStr);

		Transaction tx = db.beginTx();

		try {

			Node refNode = db.getReferenceNode();
			refNode.delete();

			Node node1 = db.createNode();
			Node node2 = db.createNode();
			Node node3 = db.createNode();
			Node node4 = db.createNode();

			node1.createRelationshipTo(node2, DynamicRelationshipType
					.withName("relType2"));
			node2.createRelationshipTo(node3, DynamicRelationshipType
					.withName("relType1"));
			node3.createRelationshipTo(node4, DynamicRelationshipType
					.withName("relType1"));
			;
			node4.createRelationshipTo(node1, DynamicRelationshipType
					.withName("relType2"));

			node1.createRelationshipTo(node3, DynamicRelationshipType
					.withName("relType2"));
			node2.createRelationshipTo(node4, DynamicRelationshipType
					.withName("relType2"));

			tx.success();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		Partitioner partitioner = new PartitionerAsSingle((byte) 0);
		NeoFromFile.applyPtnToNeo(db, partitioner);
		NeoFromFile.writeGMLBasic(db, gml0Str);

		HashSet<String> relTypes = new HashSet<String>();
		relTypes.add("relType2");
		NeoFromFile.removeRelationshipsByType(db, relTypes);

		NeoFromFile.writeGMLBasic(db, gml1Str);

		NeoFromFile.removeOrphanNodes(db);

		NeoFromFile.writeGMLBasic(db, gml2Str);

		db.shutdown();
	}

	private static void test_applyPtnToNeo() {
		String graphStr = "/home/alex/workspace/graph_gen_utils/graphs/test0.graph";
		String gml0Str = "/media/disk/alex/Neo4j/test.0.gml";
		String gml1Str = "/media/disk/alex/Neo4j/test.1.gml";
		String dbStr = "/media/disk/alex/Neo4j/test";

		Utils.cleanDir(dbStr);
		GraphDatabaseService db = new EmbeddedGraphDatabase(dbStr);

		NeoFromFile.writeNeoFromChaco(db, graphStr);

		Partitioner partitionerBal = new PartitionerAsBalanced((byte) 2);
		Partitioner partitionerRand = new PartitionerAsRandom((byte) 4);

		HashMap<String, Object> props = new HashMap<String, Object>();
		props.put(Consts.LATITUDE, 0.5d);

		NeoFromFile.applyPtnToNeo(db, partitionerBal, props);
		NeoFromFile.writeGMLBasic(db, gml0Str);

		NeoFromFile.applyPtnToNeo(db, partitionerRand, props);
		NeoFromFile.writeGMLBasic(db, gml1Str);

		db.shutdown();
	}

	private static void test_twitter_parser() {
		String pathStr = "/home/alex/workspace/graph_cluster_utils/sample dbs/";
		String fileStr = pathStr + "twitter_sarunas.sub";
		String dbDirStr = "var/twitter";

		Utils.cleanDir(dbDirStr);
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(dbDirStr);
		NeoFromFile.writeNeoFromTwitterDataset(transNeo, fileStr);
		NeoFromFile.writeMetricsCSV(transNeo, "var/twitter.met");
		transNeo.shutdown();

		// File file = new File(fileStr);
		//
		// GraphReader twitterParser = new TwitterParser(file);
		//
		// long time = System.currentTimeMillis();
		// // System.out.println("Reading Twitter Nodes Started...");
		// //
		// // long nodeCount = 0;
		// // for (NodeData node : twitterParser.getNodes()) {
		// // if (++nodeCount % 100000 == 0)
		// // System.out.println("\tNodes: " + nodeCount);
		// // }
		// // System.out.println("Nodes: " + nodeCount);
		// //
		// // System.out.printf("Reading Twitter Nodes Done...%s",
		// // getTimeStr(System
		// // .currentTimeMillis()
		// // - time));
		// //
		// // time = System.currentTimeMillis();
		// System.out.println("Reading Twitter Relationships Started...");
		//
		// long relCount = 0;
		// long selfLoopsTotal = 0;
		// long selfLoops = 0;
		// for (NodeData rel : twitterParser.getRels()) {
		// long sourceId = (Long) rel.getProperties().get(Consts.NODE_GID);
		// long destId = (Long) rel.getRelationships().get(0).get(
		// Consts.NODE_GID);
		// if (sourceId == destId)
		// selfLoops++;
		//
		// if (++relCount % 1000000 == 0) {
		// System.out.printf("\tRels [%d] Self-Loops [%d]\n", relCount,
		// selfLoops);
		// selfLoopsTotal += selfLoops;
		// selfLoops = 0;
		// }
		// }
		// System.out.printf("\tRels [%d] Self-Loops Total [%d]\n", relCount,
		// selfLoopsTotal);
		//
		// System.out.printf("Reading Twitter Relationships Done...%s",
		// getTimeStr(System.currentTimeMillis() - time));

	}

	private static void db_to_pdb(String dbDir, String pdbDir) {
		GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);
		PGraphDatabaseService pdb = NeoFromFile.writePNeoFromNeo(pdbDir, db);
		db.shutdown();
		pdb.shutdown();
	}

	private static void cleanup_GIS() {
		GraphDatabaseService romaniaNeo = new EmbeddedGraphDatabase(
				"var/romania-BAL2-GID-COORDS_ALL-CARSHORTEST");

		HashSet<String> relTypes = new HashSet<String>();
		relTypes.add("FOOT_WAY");
		// relTypes.add("BICYCLE_WAY");
		relTypes.add("CAR_WAY");
		relTypes.add("CAR_SHORTEST_WAY");

		NeoFromFile.removeRelationshipsByType(romaniaNeo, relTypes);
		NeoFromFile.removeOrphanNodes(romaniaNeo);

		do_apply_weight_all_edges(romaniaNeo);

		NeoFromFile.applyPtnToNeo(romaniaNeo, new PartitionerAsBalanced(
				(byte) 2));

		NeoFromFile
				.writeGMLBasic(romaniaNeo,
						"var/romania-balanced2-named-coords_all-carshortest-no_orphoned.basic.gml");

		romaniaNeo.shutdown();
	}

	private static void do_apply_weight_all_edges(
			GraphDatabaseService romaniaNeo) {

		long time = System.currentTimeMillis();

		System.out.printf("Get all WEIGHTs...");

		long edgeCount = 0;
		double sumWeights = 0;

		Transaction tx = romaniaNeo.beginTx();

		try {
			for (Node v : romaniaNeo.getAllNodes()) {

				for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

					Double eWeight = (Double) e.getProperty(Consts.WEIGHT, 0.0);

					edgeCount++;
					sumWeights += eWeight;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		double averageWeight = sumWeights / edgeCount;
		System.out.printf("AVERAGE_WEIGHT[%f]...", averageWeight);

		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		long maxRelationshipsPerTransaction = 100000;
		long updatedRels = maxRelationshipsPerTransaction;
		while (updatedRels >= maxRelationshipsPerTransaction) {
			time = System.currentTimeMillis();

			System.out.printf("Applying WEIGHT to relationships...");

			updatedRels = apply_weight_all_edges(romaniaNeo,
					maxRelationshipsPerTransaction, averageWeight);

			// PRINTOUT
			System.out.printf("[%d] %s", updatedRels, getTimeStr(System
					.currentTimeMillis()
					- time));
		}

	}

	private static int apply_weight_all_edges(GraphDatabaseService romaniaNeo,
			long maxRelationshipsPerTransaction, double defaultWeight) {

		int updatedRels = 0;

		Transaction tx = romaniaNeo.beginTx();

		try {
			for (Node v : romaniaNeo.getAllNodes()) {

				for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

					Double eWeight = (Double) e
							.getProperty(Consts.WEIGHT, null);

					if (eWeight != null)
						continue;

					e.setProperty(Consts.WEIGHT, defaultWeight);

					updatedRels++;
					if (updatedRels >= maxRelationshipsPerTransaction)
						break;
				}

				if (updatedRels >= maxRelationshipsPerTransaction)
					break;
			}

			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		return updatedRels;
	}

	private static void read_write_read_write_etc() {
		Utils.cleanDir("var/read_write_results");

		Utils.cleanDir("var/batchNeo0");
		Partitioner partitioner = new PartitionerAsDefault();
		NeoFromFile.writeNeoFromChacoAndPtnBatch("var/batchNeo0",
				"graphs/test0.graph", partitioner);
		GraphDatabaseService batchNeo0 = new EmbeddedGraphDatabase(
				"var/batchNeo0");
		NeoFromFile.writeChaco(batchNeo0,
				"var/read_write_results/batchNeo0.graph", ChacoType.UNWEIGHTED);
		NeoFromFile.writeGMLBasic(batchNeo0,
				"var/read_write_results/batchNeo0.basic.gml");

		Utils.cleanDir("var/neo0");
		GraphDatabaseService neo0 = new EmbeddedGraphDatabase("var/neo0");
		NeoFromFile.writeNeoFromChaco(neo0, "graphs/test0.graph");
		NeoFromFile.writeChaco(neo0, "var/read_write_results/neo0.graph",
				ChacoType.UNWEIGHTED);
		NeoFromFile
				.writeGMLBasic(neo0, "var/read_write_results/neo0.basic.gml");
		MemGraph mem0 = NeoFromFile.readMemGraph(neo0);
		NeoFromFile.writeChaco(mem0, "var/read_write_results/neo0mem00.graph",
				ChacoType.UNWEIGHTED);
		NeoFromFile.writeGMLBasic(mem0,
				"var/read_write_results/neo0mem00.basic.gml");

		Transaction tx1 = mem0.beginTx();
		try {
			mem0.setNextNodeId(9);
			MemNode memNode9 = (MemNode) mem0.createNode();

			memNode9.setNextRelId(9);
			memNode9.createRelationshipTo(mem0.getNodeById(5),
					Consts.RelationshipTypes.DEFAULT);

			((MemNode) mem0.getNodeById(5)).setNextRelId(10);
			mem0.getNodeById(5).createRelationshipTo(memNode9,
					Consts.RelationshipTypes.DEFAULT);

			((MemNode) mem0.getNodeById(5)).setNextRelId(11);
			mem0.getNodeById(5).createRelationshipTo(memNode9,
					Consts.RelationshipTypes.DEFAULT);

			NeoFromFile.writeGMLBasic(mem0,
					"var/read_write_results/neo0mem01.basic.gml");
			NeoFromFile.writeChaco(mem0,
					"var/read_write_results/neo0mem01.graph",
					ChacoType.UNWEIGHTED);

			tx1.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx1.finish();
		}

		Utils.cleanDir("var/neo1");
		GraphDatabaseService neo1 = new EmbeddedGraphDatabase("var/neo1");
		NeoFromFile.writeNeoFromGML(neo1,
				"var/read_write_results/neo0mem01.basic.gml");
		MemGraph mem1 = NeoFromFile.readMemGraph(neo1);
		NeoFromFile.writeGMLBasic(mem1,
				"var/read_write_results/neo1mem10.basic.gml");
		NeoFromFile.writeChaco(mem1, "var/read_write_results/neo1mem10.graph",
				ChacoType.UNWEIGHTED);

		Transaction tx2 = mem1.beginTx();
		try {

			Relationship memRel = mem1.getRelationshipById(3);
			System.out.printf("***Deleting MemRel[%d][%d->%d]***\n", memRel
					.getId(), memRel.getStartNode().getId(), memRel
					.getEndNode().getId());
			memRel.delete();

			tx2.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx2.finish();
		}

		NeoFromFile.writeGMLBasic(mem1,
				"var/read_write_results/neo1mem11.basic.gml");
		NeoFromFile.writeChaco(mem1, "var/read_write_results/neo1mem11.graph",
				ChacoType.UNWEIGHTED);

		Transaction tx3 = mem1.beginTx();
		try {
			MemNode memNode5 = (MemNode) mem1.getNodeById(5);
			for (Relationship memRel : memNode5
					.getRelationships(Direction.OUTGOING)) {
				System.out.printf("***MemNode[%d] MemRel[%d->%d]***\n",
						memNode5.getId(), memRel.getStartNode().getId(), memRel
								.getEndNode().getId());
			}
			System.out.println();
			for (Relationship memRel : memNode5
					.getRelationships(Direction.INCOMING)) {
				System.out.printf("***MemNode[%d] MemRel[%d<-%d]***\n",
						memNode5.getId(), memRel.getEndNode().getId(), memRel
								.getStartNode().getId());
			}
			System.out.println();
			for (Relationship memRel : memNode5
					.getRelationships(Direction.BOTH)) {
				System.out.printf("***MemNode[%d] MemRel[%d-%d]***\n", memNode5
						.getId(), memRel.getStartNode().getId(), memRel
						.getEndNode().getId());
			}
			System.out.println();
			for (Relationship memRel : memNode5.getRelationships()) {
				System.out.printf("***MemNode[%d] MemRel[%d-%d]***\n", memNode5
						.getId(), memRel.getStartNode().getId(), memRel
						.getEndNode().getId());
			}

			tx3.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx3.finish();
		}

		Utils.cleanDir("var/neo2");
		GraphDatabaseService neo2 = new EmbeddedGraphDatabase("var/neo2");
		NeoFromFile.writeNeoFromGML(neo2,
				"var/read_write_results/neo1mem11.basic.gml");
		MemGraph mem2 = NeoFromFile.readMemGraph(neo2);
		NeoFromFile.writeGMLBasic(mem2,
				"var/read_write_results/neo2mem00.basic.gml");
		NeoFromFile.writeChaco(mem2, "var/read_write_results/neo2mem00.graph",
				ChacoType.UNWEIGHTED);

		batchNeo0.shutdown();
		neo0.shutdown();
		neo1.shutdown();
		neo2.shutdown();
	}

	private static String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}

}
