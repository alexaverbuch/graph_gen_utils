package graph_gen_utils.writer.gml;

import graph_gen_utils.general.PropNames;

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

public class GMLWriterUndirectedBasic extends GMLWriter {

	public GMLWriterUndirectedBasic(File gmlFile) {
		super(gmlFile);
	}

	@Override
	public void write(GraphDatabaseService transNeo) {

		BufferedWriter bufferedWriter = null;

		Transaction tx = transNeo.beginTx();

		try {
			bufferedWriter = new BufferedWriter(new FileWriter(gmlFile));

			bufferedWriter.write("Creator \"graph_gen_utils\"\n");
			bufferedWriter.write("Version 1\n");
			bufferedWriter.write("graph\n");
			bufferedWriter.write("[\n");

			// TODO Check if "directed 1" affects visualizations
			bufferedWriter.write("\tdirected 0\n");

			for (Node node : transNeo.getAllNodes()) {

				bufferedWriter.write("\tnode\n");
				bufferedWriter.write("\t[\n");

				Long nodeId = Long.parseLong((String) node
						.getProperty(PropNames.NAME));

				bufferedWriter.write(String.format("\t\t%s %s\n", PropNames.ID,
						valueToStr(nodeId)));

				for (String propKey : node.getPropertyKeys()) {

					if ((propKey.equals(PropNames.NAME) == false)
							&& (propKey.equals(PropNames.COLOR) == false)
							&& (propKey.equals(PropNames.WEIGHT) == false))
						continue;

					Object propVal = node.getProperty(propKey);

					if (propKey.equals(PropNames.NAME))
						propVal = nodeId;

					bufferedWriter.write(String.format("\t\t%s %s\n", propKey,
							valueToStr(propVal)));
				}

				bufferedWriter.write("\t]\n");
			}

			bufferedWriter.flush();

			for (Node fromNode : transNeo.getAllNodes()) {

				Long fromId = Long.parseLong((String) fromNode
						.getProperty(PropNames.NAME));

				for (Relationship rel : fromNode
						.getRelationships(Direction.OUTGOING)) {

					Node toNode = rel.getEndNode();

					Long toId = Long.parseLong((String) toNode
							.getProperty(PropNames.NAME));

					bufferedWriter.write("\tedge\n");
					bufferedWriter.write("\t[\n");
					bufferedWriter.write(String.format("\t\t%s %s\n",
							PropNames.GML_SOURCE, valueToStr(fromId)));
					bufferedWriter.write(String.format("\t\t%s %s\n",
							PropNames.GML_TARGET, valueToStr(toId)));

					for (String propKey : rel.getPropertyKeys()) {
						if (propKey != PropNames.WEIGHT)
							continue;

						Object propVal = rel.getProperty(propKey);

						bufferedWriter.write(String.format("\t\t%s %s\n",
								propKey, valueToStr(propVal)));
					}

					Byte edgeColor = -1;

					if (fromNode.hasProperty(PropNames.COLOR)
							&& toNode.hasProperty(PropNames.COLOR)) {

						Byte fromColor = (Byte) fromNode
								.getProperty(PropNames.COLOR);
						Byte toColor = (Byte) toNode
								.getProperty(PropNames.COLOR);

						if (fromColor == toColor)
							edgeColor = fromColor;

					}

					bufferedWriter.write(String.format("\t\t%s %s\n",
							PropNames.COLOR, valueToStr(edgeColor)));

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
