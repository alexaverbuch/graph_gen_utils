package applications;

import graph_gen_utils.NeoFromFile;

import java.util.HashSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class SimplifyGraphs {
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    
    if (args[0].equals("help")) {
      System.out.println("Params - " + "RelTypesToRemove:Str "
        + "Neo4jDirectories:Str");
      System.out.println("E.g. - " + "{RelType1,RelType2,...,RelTypeN} "
        + "Neo4jDir1/ Neo4jDir2/ ...");
      return;
    }
    
    String[] relTypes = args[0].replaceAll("[{}]", "").split("[,]");
    
    HashSet<String> removalRelTypes = new HashSet<String>();
    for (String relType : relTypes)
      removalRelTypes.add(relType);
    
    for (int i = 1; i < args.length; i++) {
      String dbDir = args[i];
      
      GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);
      
      try {
        
        long time = System.currentTimeMillis();
        System.out.printf("Simplifying %s\n\t", dbDir);
        
        NeoFromFile.removeRelationshipsByType(db, removalRelTypes);
        
        System.out.printf("\t");
        
        NeoFromFile.removeDuplicateRelationships(db, Direction.OUTGOING);
        
        System.out.printf("\t");
        
        NeoFromFile.removeOrphanNodes(db);
        
        System.out.printf("Time Taken: %s", getTimeStr(System
          .currentTimeMillis()
          - time));
        
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        db.shutdown();
      }
    }
    
  }
  
  private static String getTimeStr(long msTotal) {
    long ms = msTotal % 1000;
    long s = (msTotal / 1000) % 60;
    long m = (msTotal / 1000) / 60;
    
    return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
  }
  
}
