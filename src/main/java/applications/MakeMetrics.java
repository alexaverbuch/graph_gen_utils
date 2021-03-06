package applications;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.memory_graph.MemGraph;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class MakeMetrics {
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    
    if (args[0].equals("help")) {
      System.out.println("Params - " + "MetricsFilePath:Str "
        + "Neo4jDirectory:Str");
      return;
    }
    
    String metricsPath = args[0];
    String dbDir = args[1];
    
    GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);
    MemGraph memDb = NeoFromFile.readMemGraph(db);
    NeoFromFile.writeMetricsCSV(memDb, metricsPath);
    
    db.shutdown();
    
  }
  
}
