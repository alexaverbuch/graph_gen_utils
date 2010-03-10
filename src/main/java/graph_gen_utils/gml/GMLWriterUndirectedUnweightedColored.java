package graph_gen_utils.gml;

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

public class GMLWriterUndirectedUnweightedColored extends GMLWriter {

	@Override
	public void write(GraphDatabaseService transNeo, File gmlFile) {

		BufferedWriter bufferedWriter = null;

		Transaction tx = transNeo.beginTx();

		try {
			bufferedWriter = new BufferedWriter(new FileWriter(gmlFile));

			bufferedWriter.write("Creator \"graph_gen_utils\"\n");
			bufferedWriter.write("Version 1\n");
			bufferedWriter.write("graph\n");
			bufferedWriter.write("[\n");
			bufferedWriter.write("\tdirected 0\n");

			for (Node node : transNeo.getAllNodes()) {
				if (node.getId() == 0)
					continue;

				bufferedWriter.write("\tnode\n");
				bufferedWriter.write("\t[\n");
				bufferedWriter
						.write(String.format("\t\tid %d\n", node.getId()));
				bufferedWriter.write(String.format("\t\tcolor %d\n",
						(Byte) node.getProperty("color")));
				bufferedWriter.write("\t]\n");
			}

			bufferedWriter.flush();

			for (Node node : transNeo.getAllNodes()) {
				if (node.getId() == 0)
					continue;

				Long fromId = node.getId();

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {

					Long toId = rel.getEndNode().getId();

					// Only count each edge once. Underlying neo4j
					// graph has 2 directed edges for each undirected edge
					if (fromId > toId)
						continue;

					bufferedWriter.write("\tedge\n");
					bufferedWriter.write("\t[\n");
					bufferedWriter.write(String.format("\t\tsource %d\n",
							fromId));
					bufferedWriter
							.write(String.format("\t\ttarget %d\n", toId));
					bufferedWriter.write("\t]\n");

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
