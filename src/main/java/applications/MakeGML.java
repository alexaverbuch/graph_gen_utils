package applications;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.memory_graph.MemGraph;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class MakeGML {
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    
    if (args[0].equals("help")) {
      System.out.println("Params - " + "GMLFilePath:Str "
        + "Neo4jDirectory:Str");
      return;
    }
    
    String gmlPath = args[0];
    String dbDir = args[1];
    
    GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);
    NeoFromFile.writeGMLBasic(db, gmlPath);
    
    db.shutdown();
    
  }
  
}
