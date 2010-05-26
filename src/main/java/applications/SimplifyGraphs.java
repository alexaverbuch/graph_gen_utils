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
			System.out.println("Params - " + "RelTypesToRemove:Tuple2 "
					+ "RandomRelDeleteParams:Array "
					+ "RemoveDuplicateRelationships:Bool "
					+ "RemoveOrphanNodes:Bool " + "Neo4jDirectories:Array");
			System.out.println("E.g. - " + "{RelType1:RelType2:...:RelTypeN} "
					+ "{PercentToKeep:MaxId} " + "true " + "true "
					+ "Neo4jDir1/ Neo4jDir2/ ...");
			return;
		}

		String[] relTypes = args[0].replaceAll("[{}]", "").split("[:]");
		HashSet<String> removalRelTypes = new HashSet<String>();
		for (String relType : relTypes) {
			if (relType.isEmpty() == true)
				continue;
			removalRelTypes.add(relType);
		}

		String[] randomDeleteParams = args[1].replaceAll("[{}]", "").split(
				"[:]");
		if ((randomDeleteParams.length != 0)
				&& (randomDeleteParams.length != 2)) {
			System.out.printf("Invalid parameter: %s\n", args[1]);
			return;
		}
		double percRelsToKeep = Double.parseDouble(randomDeleteParams[0]);
		if ((percRelsToKeep < 0) || (percRelsToKeep >= 1.0)) {
			System.out.println("PercentToDelete must be in range [0,1)");
			return;
		}
		int maxRelId = Integer.parseInt(randomDeleteParams[1]);

		boolean removeDuplicateRelationships = Boolean.parseBoolean(args[2]);

		boolean removeOrphanNodes = Boolean.parseBoolean(args[3]);

		for (int i = 4; i < args.length; i++) {
			String dbDir = args[i];

			GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);

			try {

				long time = System.currentTimeMillis();
				System.out.printf("Simplifying %s\n", dbDir);

				if (removalRelTypes.size() > 0) {
					System.out.printf("\t");
					NeoFromFile.removeRelationshipsByType(db, removalRelTypes);
				}

				if (percRelsToKeep > 0) {
					System.out.printf("\t");
					NeoFromFile.removeRandomRelationships(db, percRelsToKeep,
							maxRelId);
				}

				if (removeDuplicateRelationships == true) {
					System.out.printf("\t");
					NeoFromFile.removeDuplicateRelationships(db,
							Direction.OUTGOING);
				}

				if (removeOrphanNodes == true) {
					System.out.printf("\t");
					NeoFromFile.removeOrphanNodes(db);
				}

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
