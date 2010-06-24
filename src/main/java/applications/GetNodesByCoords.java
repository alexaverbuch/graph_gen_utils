package applications;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.general.Consts;
import graph_gen_utils.memory_graph.MemGraph;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class GetNodesByCoords {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args[0].equals("help")) {
			System.out.println("Params - " + "OutputDir:Str "
					+ "Neo4jDirectory:Str");
			return;
		}

		String outputDir = args[0];
		String dbDir = args[1];

		BufferedWriter bufferedWriterLon = null;
		BufferedWriter bufferedWriterLat = null;
		GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);

		Transaction tx = db.beginTx();

		try {
			bufferedWriterLon = new BufferedWriter(new FileWriter(new File(
					outputDir + "/lon.txt")));
			bufferedWriterLat = new BufferedWriter(new FileWriter(new File(
					outputDir + "/lat.txt")));

			for (Node node : db.getAllNodes()) {
				double nodeLon = (Double) node.getProperty(Consts.LONGITUDE);
				bufferedWriterLon.write(Double.toString(nodeLon));
				bufferedWriterLon.newLine();
				double nodeLat = (Double) node.getProperty(Consts.LATITUDE);
				bufferedWriterLat.write(Double.toString(nodeLat));
				bufferedWriterLat.newLine();
			}

			tx.finish();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
			db.shutdown();

			// Close the BufferedWriter
			try {
				if (bufferedWriterLon != null) {
					bufferedWriterLon.flush();
					bufferedWriterLon.close();
				}
				if (bufferedWriterLat != null) {
					bufferedWriterLat.flush();
					bufferedWriterLat.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}

}
