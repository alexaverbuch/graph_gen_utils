package graph_gen_utils.writer.gml;

import graph_gen_utils.general.Consts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class GMLWriterUndirectedBasic extends GMLWriter {
  
  public GMLWriterUndirectedBasic(File gmlFile) {
    super(gmlFile);
  }
  
  @Override
  public void write(GraphDatabaseService transNeo) {
    
    BufferedWriter bufferedWriter = null;
    
    Transaction tx = transNeo.beginTx();
    
    try {
      bufferedWriter = new BufferedWriter(new FileWriter(gmlFile));
      
      bufferedWriter.write("Creator \"graph_gen_utils\"\n");
      bufferedWriter.write("Version 1\n");
      bufferedWriter.write("graph\n");
      bufferedWriter.write("[\n");
      
      // TODO Check if "directed 1" affects visualizations
      bufferedWriter.write("\tdirected 0\n");
      
      int flushBuffer = 0;
      
      for (Node node : transNeo.getAllNodes()) {
        
        flushBuffer++;
        
        bufferedWriter.write("\tnode\n");
        bufferedWriter.write("\t[\n");
        
        Long nodeId = (Long) node.getProperty(Consts.NODE_GID);
        
        bufferedWriter.write(String.format("\t\t%s %s\n", Consts.GML_ID,
          valToStr(nodeId)));
        
        for (String propKey : node.getPropertyKeys()) {
          
          if ((propKey.equals(Consts.NODE_GID) == false)
            && (propKey.equals(Consts.COLOR) == false)
            && (propKey.equals(Consts.WEIGHT) == false)
            && (propKey.equals(Consts.LATITUDE) == false)
            && (propKey.equals(Consts.LONGITUDE) == false))
            continue;
          
          Object propVal = node.getProperty(propKey);
          
          if (propKey.equals(Consts.NODE_GID))
            propVal = nodeId;
          
          bufferedWriter.write(String.format("\t\t%s %s\n",
            removeIllegalKeyChars(propKey), valToStr(propVal)));
        }
        
        bufferedWriter.write("\t]\n");
        
        // Temporary flush to reduce memory consumption
        if (flushBuffer % Consts.STORE_BUF == 0) {
          bufferedWriter.flush();
        }
        
      }
      
      bufferedWriter.flush();
      
      flushBuffer = 0;
      
      for (Node fromNode : transNeo.getAllNodes()) {
        
        Long fromId = (Long) fromNode.getProperty(Consts.NODE_GID);
        
        for (Relationship rel : fromNode.getRelationships(Direction.OUTGOING)) {
          
          flushBuffer++;
          
          Node toNode = rel.getEndNode();
          
          Long toId = (Long) toNode.getProperty(Consts.NODE_GID);
          
          bufferedWriter.write("\tedge\n");
          bufferedWriter.write("\t[\n");
          bufferedWriter.write(String.format("\t\t%s %s\n", Consts.GML_SOURCE,
            valToStr(fromId)));
          bufferedWriter.write(String.format("\t\t%s %s\n", Consts.GML_TARGET,
            valToStr(toId)));
          
          Long relGid = rel.getId();
          for (String propKey : rel.getPropertyKeys()) {
            if ((propKey.equals(Consts.WEIGHT) == false)
              && (propKey.equals(Consts.REL_GID) == false))
              continue;
            
            if (propKey.equals(Consts.REL_GID)) {
              relGid = (Long) rel.getProperty(propKey);
              continue;
            }
            
            Object propVal = rel.getProperty(propKey);
            
            bufferedWriter.write(String.format("\t\t%s %s\n",
              removeIllegalKeyChars(propKey), valToStr(propVal)));
          }
          
          bufferedWriter.write(String.format("\t\t%s %s\n",
            removeIllegalKeyChars(Consts.REL_GID), valToStr(relGid)));
          
          Byte edgeColor = -1;
          
          if (fromNode.hasProperty(Consts.COLOR)
            && toNode.hasProperty(Consts.COLOR)) {
            
            Byte fromColor = (Byte) fromNode.getProperty(Consts.COLOR);
            Byte toColor = (Byte) toNode.getProperty(Consts.COLOR);
            
            if (fromColor == toColor)
              edgeColor = fromColor;
            
          }
          
          bufferedWriter.write(String.format("\t\t%s %s\n", Consts.COLOR,
            valToStr(edgeColor)));
          
          bufferedWriter.write("\t]\n");
          
          // Temporary flush to reduce memory consumption
          if (flushBuffer % Consts.STORE_BUF == 0) {
            bufferedWriter.flush();
          }
          
        }
        
      }
      
      bufferedWriter.write("]");
      
    } catch (FileNotFoundException ex) {
      ex.printStackTrace();
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      
      tx.finish();
      
      // Close the BufferedWriter
      try {
        if (bufferedWriter != null) {
          bufferedWriter.flush();
          bufferedWriter.close();
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
    
  }
}
