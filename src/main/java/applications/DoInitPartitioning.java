package applications;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.partitioner.Partitioner;
import graph_gen_utils.partitioner.PartitionerAsBalanced;
import graph_gen_utils.partitioner.PartitionerAsRandom;
import graph_gen_utils.partitioner.PartitionerAsSingle;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class DoInitPartitioning {
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    
    if (args[0].equals("help")) {
      System.out.println("Params - " + "PartitionType:Atom " + "PtnVal:Int "
        + "Neo4jDirectory:Str");
      System.out.println("E.g. " + "random|balanced|single " + "2 " + "var/");
      return;
    }
    
    String ptnType = args[0];
    Byte ptnVal = Byte.parseByte(args[1]);
    String dbDir = args[2];
    
    Partitioner partitioner = null;
    
    if (ptnType.equals("random"))
      partitioner = new PartitionerAsRandom(ptnVal);
    else if (ptnType.equals("balanced"))
      partitioner = new PartitionerAsBalanced(ptnVal);
    else if (ptnType.equals("single"))
      partitioner = new PartitionerAsSingle(ptnVal);
    else {
      System.out.printf("Invalid PartitionType: %s\n", ptnType);
      return;
    }
    
    GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);
    NeoFromFile.applyPtnToNeo(db, partitioner);
    
    db.shutdown();
    
  }
  
  private static String getTimeStr(long msTotal) {
    long ms = msTotal % 1000;
    long s = (msTotal / 1000) % 60;
    long m = (msTotal / 1000) / 60;
    
    return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
  }
  
}
