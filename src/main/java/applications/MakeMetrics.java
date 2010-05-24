package applications;

import graph_gen_utils.NeoFromFile;

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
    
    NeoFromFile.writeMetricsCSV(db, metricsPath);
    
    db.shutdown();
    
  }
  
  private static String getTimeStr(long msTotal) {
    long ms = msTotal % 1000;
    long s = (msTotal / 1000) % 60;
    long m = (msTotal / 1000) / 60;
    
    return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
  }
  
}
