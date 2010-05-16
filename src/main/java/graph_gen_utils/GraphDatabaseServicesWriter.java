package graph_gen_utils;

import graph_gen_utils.general.Consts;
import graph_gen_utils.general.NodeData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

class GraphDatabaseServicesWriter {
  
  private GraphDatabaseService transNeo = null;
  private IndexService transIndexService = null;
  private BatchInserter batchNeo = null;
  private LuceneIndexBatchInserter batchIndexService = null;
  private String dbDir = null;
  private boolean isTransactional = true;
  
  public static GraphDatabaseServicesWriter createTransactionalWrapper(
    GraphDatabaseService transNeo) {
    return new GraphDatabaseServicesWriter(transNeo, new LuceneIndexService(
      transNeo));
  }
  
  public static GraphDatabaseServicesWriter createTransactionalWrapper(
    String dbDir) {
    GraphDatabaseService transNeo = new EmbeddedGraphDatabase(dbDir);
    return new GraphDatabaseServicesWriter(transNeo, new LuceneIndexService(
      transNeo));
  }
  
  public static GraphDatabaseServicesWriter createBatchWrapper(String dbDir) {
    BatchInserter batchNeo = new BatchInserterImpl(dbDir);
    // new BatchInserterImpl("db dir", BatchInserterImpl
    // .loadProperties("neo.props"));
    return new GraphDatabaseServicesWriter(batchNeo,
      new LuceneIndexBatchInserterImpl(batchNeo), dbDir);
  }
  
  private GraphDatabaseServicesWriter(GraphDatabaseService transNeo,
    IndexService transIndexService) {
    
    if (isSupportedGraphDatabaseService(transNeo) == false)
      throw new UnsupportedOperationException(
        "GraphDatabaseService implementation not supported");
    
    this.transNeo = transNeo;
    this.transIndexService = transIndexService;
    this.batchNeo = null;
    this.batchIndexService = null;
    this.dbDir = null;
    this.isTransactional = true;
  }
  
  private GraphDatabaseServicesWriter(BatchInserter batchNeo,
    LuceneIndexBatchInserter batchIndexService, String dbDir) {
    this.transNeo = null;
    this.transIndexService = null;
    this.batchNeo = batchNeo;
    this.batchIndexService = batchIndexService;
    this.dbDir = dbDir;
    this.isTransactional = false;
  }
  
  public void flushNodes(ArrayList<NodeData> nodes) throws Exception {
    if (isTransactional == true)
      flushNodesTrans(transIndexService, transNeo, nodes);
    else
      flushNodesBatch(batchIndexService, batchNeo, nodes);
  }
  
  public void flushRels(ArrayList<NodeData> nodes) throws Exception {
    if (isTransactional == true)
      flushRelsTrans(transIndexService, transNeo, nodes);
    else
      flushRelsBatch(batchIndexService, batchNeo, nodes);
  }
  
  public void removeReferenceNode() throws Exception {
    // Used when reading from input graph (E.g. Chaco, GML, Topology etc)
    // Not used when working on existing Neo4j instance
    
    if (ensureTransactionalDbOpen() == false)
      throw new Exception(
        "Reference Node can not be removed without GraphDatabaseService");
    
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
  
  public void optimizeIndex() {
    if (isTransactional == false)
      batchIndexService.optimize();
  }
  
  public void shutdownIndex() {
    if (transIndexService != null) {
      transIndexService.shutdown();
      transIndexService = null;
    }
    if (batchIndexService != null) {
      batchIndexService.shutdown();
      batchIndexService = null;
    }
  }
  
  public void shutdownDbAndIndex() {
    if (transIndexService != null) {
      transIndexService.shutdown();
      transIndexService = null;
    }
    if (transNeo != null) {
      transNeo.shutdown();
      transNeo = null;
    }
    if (batchIndexService != null) {
      batchIndexService.shutdown();
      batchIndexService = null;
    }
    if (batchNeo != null) {
      batchNeo.shutdown();
      batchNeo = null;
    }
  }
  
  private boolean ensureTransactionalDbOpen() {
    if (transNeo != null)
      return true;
    
    if (dbDir == null)
      return false;
    
    shutdownDbAndIndex();
    
    transNeo = new EmbeddedGraphDatabase(dbDir);
    transIndexService = new LuceneIndexService(transNeo);
    return true;
  }
  
  private void flushNodesBatch(LuceneIndexBatchInserter batchIndexService,
    BatchInserter batchNeo, ArrayList<NodeData> nodes) throws Exception {
    
    for (NodeData nodeAndRels : nodes) {
      long nodeID = batchNeo.createNode(nodeAndRels.getProperties());
      
      for (Entry<String, Object> nodeProp : nodeAndRels.getProperties()
        .entrySet()) {
        batchIndexService.index(nodeID, nodeProp.getKey(), nodeProp.getValue());
      }
      
    }
  }
  
  private void flushRelsBatch(LuceneIndexBatchInserter batchIndexService,
    BatchInserter batchNeo, ArrayList<NodeData> nodes) throws Exception {
    
    // HashMap<String, Object> emptyProps = new HashMap<String, Object>();
    
    for (NodeData nodeAndRels : nodes) {
      long fromNodeId =
        batchIndexService.getSingleNode(Consts.NODE_GID, nodeAndRels
          .getProperties().get(Consts.NODE_GID));
      
      for (Map<String, Object> rel : nodeAndRels.getRelationships()) {
        // Get end Node
        long toNodeId =
          batchIndexService.getSingleNode(Consts.NODE_GID, (Long) rel
            .get(Consts.NODE_GID));
        
        // Create Relationship
        // long relId =
        // batchNeo.createRelationship(fromNodeId, toNodeId,
        // Consts.RelationshipTypes.DEFAULT, emptyProps);
        batchNeo.createRelationship(fromNodeId, toNodeId,
          Consts.RelationshipTypes.DEFAULT, rel);
        
        // Set properties on Relationship
        // Object relGid = rel.get(Consts.REL_GID);
        // relId = (relGid == null) ? relId : (Long) relGid;
        // rel.put(Consts.REL_GID, relId);
        // batchNeo.setRelationshipProperties(relId, rel);
        
      }
    }
  }
  
  private void flushNodesTrans(IndexService transIndexService,
    GraphDatabaseService transNeo, ArrayList<NodeData> nodes) {
    
    Transaction tx = transNeo.beginTx();
    
    try {
      for (NodeData nodeAndRels : nodes) {
        Node node = transNeo.createNode();
        
        for (Entry<String, Object> nodeProp : nodeAndRels.getProperties()
          .entrySet()) {
          
          node.setProperty(nodeProp.getKey(), nodeProp.getValue());
          transIndexService.index(node, nodeProp.getKey(), nodeProp.getValue());
          
        }
        
      }
      
      tx.success();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tx.finish();
    }
    
  }
  
  private void flushRelsTrans(IndexService transIndexService,
    GraphDatabaseService transNeo, ArrayList<NodeData> nodes) {
    
    Transaction tx = transNeo.beginTx();
    
    Long fromId = null;
    Long toId = null;
    
    try {
      
      for (NodeData nodeAndRels : nodes) {
        fromId = (Long) nodeAndRels.getProperties().get(Consts.NODE_GID);
        Node fromNode =
          transIndexService.getSingleNode(Consts.NODE_GID, fromId);
        
        for (Map<String, Object> rel : nodeAndRels.getRelationships()) {
          toId = (Long) rel.get(Consts.NODE_GID);
          
          Node toNode = transIndexService.getSingleNode(Consts.NODE_GID, toId);
          
          Relationship neoRel =
            fromNode.createRelationshipTo(toNode,
              Consts.RelationshipTypes.DEFAULT);
          
          // Long relGID = neoRel.getId();
          
          for (Entry<String, Object> relProp : rel.entrySet()) {
            
            String relPropKey = relProp.getKey();
            
            if (relPropKey.equals(Consts.NODE_GID))
              continue;
            
            // if (relPropKey.equals(Consts.REL_GID)) {
            // relGID = (Long) relProp.getValue();
            // continue;
            // }
            
            neoRel.setProperty(relPropKey, relProp.getValue());
            
          }
          
          // neoRel.setProperty(Consts.REL_GID, relGID);
          
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
  
  private boolean isSupportedGraphDatabaseService(GraphDatabaseService transNeo) {
    
    if (transNeo.getClass().equals(EmbeddedGraphDatabase.class))
      return true;
    
    return false;
  }
  
}
