package dodgy_tests;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.HashSet;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ChacoType;
import graph_gen_utils.general.Consts;
import graph_gen_utils.general.DirUtils;
import graph_gen_utils.general.NodeData;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.memory_graph.MemNode;
import graph_gen_utils.partitioner.Partitioner;
import graph_gen_utils.partitioner.PartitionerAsBalanced;
import graph_gen_utils.partitioner.PartitionerAsRandom;
import graph_gen_utils.partitioner.PartitionerAsSingle;
import graph_gen_utils.reader.GraphReader;
import graph_gen_utils.reader.twitter.TwitterParser;

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
    test_delete_edge_types();
  }
  
  private static void test_delete_edge_types() {
    String dbStr = "/media/disk/alex/Neo4j/test";
    String gml0Str = "/media/disk/alex/Neo4j/test.0.gml";
    String gml1Str = "/media/disk/alex/Neo4j/test.1.gml";
    String gml2Str = "/media/disk/alex/Neo4j/test.2.gml";
    
    DirUtils.cleanDir(dbStr);
    GraphDatabaseService db = new EmbeddedGraphDatabase(dbStr);
    
    Transaction tx = db.beginTx();
    
    RelationshipType relType1 = DynamicRelationshipType.withName("RelType1");
    RelationshipType relType2 = DynamicRelationshipType.withName("RelType2");
    
    try {
      
      Node refNode = db.getReferenceNode();
      refNode.delete();
      
      Node node1 = db.createNode();
      Node node2 = db.createNode();
      Node node3 = db.createNode();
      Node node4 = db.createNode();
      
      node1.createRelationshipTo(node2, relType2);
      node2.createRelationshipTo(node3, relType1);
      node3.createRelationshipTo(node4, relType1);
      node4.createRelationshipTo(node1, relType2);
      
      node1.createRelationshipTo(node3, relType2);
      node2.createRelationshipTo(node4, relType2);
      
      tx.success();
      
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tx.finish();
    }
    
    Partitioner partitioner = new PartitionerAsSingle((byte) 0);
    NeoFromFile.applyPtnToNeo(db, partitioner);
    NeoFromFile.writeGMLBasic(db, gml0Str);
    
    HashSet<RelationshipType> relTypes = new HashSet<RelationshipType>();
    relTypes.add(relType2);
    NeoFromFile.removeRelationshipsByType(db, relTypes);
    
    NeoFromFile.writeGMLBasic(db, gml1Str);
    
    NeoFromFile.removeOrphanNodes(db);
    
    NeoFromFile.writeGMLBasic(db, gml2Str);
    
    db.shutdown();
  }
  
  private static void test_applyPtnToNeo() {
    String graphStr = "/home/alex/workspace/graph_gen_utils/graphs/test0.graph";
    String gmlStr = "/media/disk/alex/Neo4j/test.gml";
    String dbStr = "/media/disk/alex/Neo4j/test";
    
    DirUtils.cleanDir(dbStr);
    GraphDatabaseService db = new EmbeddedGraphDatabase(dbStr);
    NeoFromFile.writeNeoFromChaco(db, graphStr);
    Partitioner partitioner = new PartitionerAsBalanced((byte) 2);
    HashMap<String, Object> props = new HashMap<String, Object>();
    props.put(Consts.LATITUDE, 0.5d);
    NeoFromFile.applyPtnToNeo(db, partitioner, props);
    NeoFromFile.writeGMLBasic(db, gmlStr);
    db.shutdown();
  }
  
  private static void test_twitter_parser() {
    String pathStr = "/home/alex/workspace/graph_cluster_utils/sample dbs/";
    String fileStr = pathStr + "twitter_sarunas.sub";
    String dbDirStr = "var/twitter";
    
    DirUtils.cleanDir(dbDirStr);
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
    GraphDatabaseService romaniaNeo =
      new EmbeddedGraphDatabase("var/romania-BAL2-GID-COORDS_ALL-CARSHORTEST");
    
    HashSet<RelationshipType> relTypes = new HashSet<RelationshipType>();
    relTypes.add(DynamicRelationshipType.withName("FOOT_WAY"));
    relTypes.add(DynamicRelationshipType.withName("BICYCLE_WAY"));
    relTypes.add(DynamicRelationshipType.withName("CAR_WAY"));
    relTypes.add(DynamicRelationshipType.withName("CAR_SHORTEST_WAY"));
    
    NeoFromFile.removeRelationshipsByType(romaniaNeo, relTypes);
    NeoFromFile.removeOrphanNodes(romaniaNeo);
    
    do_apply_weight_all_edges(romaniaNeo);
    
    NeoFromFile.applyPtnToNeo(romaniaNeo, new PartitionerAsBalanced((byte) 2));
    
    NeoFromFile
      .writeGMLBasic(romaniaNeo,
        "var/romania-balanced2-named-coords_all-carshortest-no_orphoned.basic.gml");
    
    romaniaNeo.shutdown();
  }
  
  private static void do_apply_weight_all_edges(GraphDatabaseService romaniaNeo) {
    
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
      
      updatedRels =
        apply_weight_all_edges(romaniaNeo, maxRelationshipsPerTransaction,
          averageWeight);
      
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
          
          Double eWeight = (Double) e.getProperty(Consts.WEIGHT, null);
          
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
    DirUtils.cleanDir("var/read_write_results");
    
    DirUtils.cleanDir("var/neo0");
    GraphDatabaseService neo0 = new EmbeddedGraphDatabase("var/neo0");
    NeoFromFile.writeNeoFromChaco(neo0, "graphs/test0.graph");
    NeoFromFile.writeChaco(neo0, "var/read_write_results/neo0.graph",
      ChacoType.UNWEIGHTED);
    NeoFromFile.writeGMLBasic(neo0, "var/read_write_results/neo0.basic.gml");
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
      NeoFromFile.writeChaco(mem0, "var/read_write_results/neo0mem01.graph",
        ChacoType.UNWEIGHTED);
      
      tx1.success();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tx1.finish();
    }
    
    DirUtils.cleanDir("var/neo1");
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
      System.out.printf("***Deleting MemRel[%d][%d->%d]***\n", memRel.getId(),
        memRel.getStartNode().getId(), memRel.getEndNode().getId());
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
      for (Relationship memRel : memNode5.getRelationships(Direction.OUTGOING)) {
        System.out.printf("***MemNode[%d] MemRel[%d->%d]***\n", memNode5
          .getId(), memRel.getStartNode().getId(), memRel.getEndNode().getId());
      }
      System.out.println();
      for (Relationship memRel : memNode5.getRelationships(Direction.INCOMING)) {
        System.out.printf("***MemNode[%d] MemRel[%d<-%d]***\n", memNode5
          .getId(), memRel.getEndNode().getId(), memRel.getStartNode().getId());
      }
      System.out.println();
      for (Relationship memRel : memNode5.getRelationships(Direction.BOTH)) {
        System.out.printf("***MemNode[%d] MemRel[%d-%d]***\n",
          memNode5.getId(), memRel.getStartNode().getId(), memRel.getEndNode()
            .getId());
      }
      System.out.println();
      for (Relationship memRel : memNode5.getRelationships()) {
        System.out.printf("***MemNode[%d] MemRel[%d-%d]***\n",
          memNode5.getId(), memRel.getStartNode().getId(), memRel.getEndNode()
            .getId());
      }
      
      tx3.success();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tx3.finish();
    }
    
    DirUtils.cleanDir("var/neo2");
    GraphDatabaseService neo2 = new EmbeddedGraphDatabase("var/neo2");
    NeoFromFile.writeNeoFromGML(neo2,
      "var/read_write_results/neo1mem11.basic.gml");
    MemGraph mem2 = NeoFromFile.readMemGraph(neo2);
    NeoFromFile.writeGMLBasic(mem2,
      "var/read_write_results/neo2mem00.basic.gml");
    NeoFromFile.writeChaco(mem2, "var/read_write_results/neo2mem00.graph",
      ChacoType.UNWEIGHTED);
    
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
