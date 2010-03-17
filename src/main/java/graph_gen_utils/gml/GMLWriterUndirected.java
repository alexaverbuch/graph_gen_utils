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

public class GMLWriterUndirected extends GMLWriter {

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

				// NOTE new
				Long nodeId = node.getId() - 1;
				// String nodeId = (String) node.getProperty("name");
				// bufferedWriter.write(String.format("\t\tid %s\n", nodeId));
				bufferedWriter.write(String.format("\t\tid %d\n", nodeId));
				// NOTE old
				// bufferedWriter
				// .write(String.format("\t\tid %d\n", node.getId()));

				// NOTE new
				for (String propKey : node.getPropertyKeys()) {
					Object propVal = node.getProperty(propKey);

					if (propKey.equals("name"))
						// propVal = nodeId;
						// propVal = node.getId();
						propVal = nodeId;

					bufferedWriter.write(String.format("\t\t%s %s\n", propKey,
							propVal));
				}
				// NOTE old
				// bufferedWriter.write(String.format("\t\tcolor %d\n",
				// (Byte) node.getProperty("color")));

				bufferedWriter.write("\t]\n");
			}

			bufferedWriter.flush();

			for (Node fromNode : transNeo.getAllNodes()) {
				if (fromNode.getId() == 0)
					continue;

				// NOTE new
				// String fromId = (String) fromNode.getProperty("name");
				Long fromId = fromNode.getId() - 1;
				// NOTE old
				// Long fromId = fromNode.getId();

				for (Relationship rel : fromNode
						.getRelationships(Direction.OUTGOING)) {

					Node toNode = rel.getEndNode();

					// NOTE new
					// String toId = (String) toNode.getProperty("name");
					Long toId = toNode.getId() - 1;
					// NOTE old
					// Long toId = toNode.getId();

					// Only count each edge once. Underlying neo4j
					// graph has 2 directed edges for each undirected edge
					// NOTE new
					if (fromId > toId)
						continue;
					// NOTE new
					// if (Long.parseLong(fromId) > Long.parseLong(toId))
					// continue;

					bufferedWriter.write("\tedge\n");
					bufferedWriter.write("\t[\n");
					bufferedWriter.write(String.format("\t\tsource %s\n",
							fromId));
					bufferedWriter
							.write(String.format("\t\ttarget %s\n", toId));

					// NOTE new
					for (String propKey : rel.getPropertyKeys()) {
						Object propVal = rel.getProperty(propKey);

						bufferedWriter.write(String.format("\t\t%s %s\n",
								propKey, propVal));
					}

					if (fromNode.hasProperty("color")
							&& toNode.hasProperty("color")) {

						Byte edgeColor = -1;

						Byte fromColor = (Byte) fromNode.getProperty("color");
						Byte toColor = (Byte) toNode.getProperty("color");

						if (fromColor == toColor)
							edgeColor = fromColor;

						bufferedWriter.write(String.format("\t\t%s %d\n",
								"color", edgeColor));
					}

					// NOTE old
					// bufferedWriter.write(String.format("\t\tcolor %d\n",
					// edgeColor));

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
