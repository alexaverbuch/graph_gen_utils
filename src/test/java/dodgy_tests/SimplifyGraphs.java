package dodgy_tests;

import graph_gen_utils.NeoFromFile;

import java.util.HashSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class SimplifyGraphs {

	public enum GISRelationshipTypes implements RelationshipType {
		FOOT_WAY, BICYCLE_WAY, CAR_WAY, CAR_SHORTEST_WAY
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		for (String dbDir : args) {

			GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);

			try {

				HashSet<String> removalRelTypes = new HashSet<String>();
				removalRelTypes.add("FOOT_WAY");
				removalRelTypes.add("CAR_WAY");
				removalRelTypes.add("CAR_SHORTEST_WAY");

				long time = System.currentTimeMillis();
				System.out.printf("Simplifying %s\n\t", dbDir);

				NeoFromFile.removeRelationshipsByType(db, removalRelTypes);

				System.out.printf("\t");

				NeoFromFile
						.removeDuplicateRelationships(db, Direction.OUTGOING);

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
