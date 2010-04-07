package graph_gen_utils.writer.gml;

import graph_gen_utils.general.Consts;

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

public class GMLWriterUndirectedFull extends GMLWriter {

	public GMLWriterUndirectedFull(File gmlFile) {
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
						.getProperty(Consts.NAME));

				bufferedWriter.write(String.format("\t\t%s %s\n", Consts.ID,
						valueToStr(nodeId)));

				for (String propKey : node.getPropertyKeys()) {
					Object propVal = node.getProperty(propKey);

					if (propKey.equals(Consts.NAME))
						propVal = nodeId;

					bufferedWriter.write(String.format("\t\t%s %s\n", propKey,
							valueToStr(propVal)));
				}

				bufferedWriter.write("\t]\n");
			}

			bufferedWriter.flush();

			for (Node fromNode : transNeo.getAllNodes()) {

				Long fromId = Long.parseLong((String) fromNode
						.getProperty(Consts.NAME));

				for (Relationship rel : fromNode
						.getRelationships(Direction.OUTGOING)) {

					Node toNode = rel.getEndNode();

					Long toId = Long.parseLong((String) toNode
							.getProperty(Consts.NAME));

					bufferedWriter.write("\tedge\n");
					bufferedWriter.write("\t[\n");
					bufferedWriter.write(String.format("\t\t%s %s\n",
							Consts.GML_SOURCE, valueToStr(fromId)));
					bufferedWriter.write(String.format("\t\t%s %s\n",
							Consts.GML_TARGET, valueToStr(toId)));

					for (String propKey : rel.getPropertyKeys()) {
						Object propVal = rel.getProperty(propKey);

						bufferedWriter.write(String.format("\t\t%s %s\n",
								propKey, valueToStr(propVal)));
					}

					Byte edgeColor = -1;

					if (fromNode.hasProperty(Consts.COLOR)
							&& toNode.hasProperty(Consts.COLOR)) {

						Byte fromColor = (Byte) fromNode
								.getProperty(Consts.COLOR);
						Byte toColor = (Byte) toNode
								.getProperty(Consts.COLOR);

						if (fromColor == toColor)
							edgeColor = fromColor;

					}

					bufferedWriter.write(String.format("\t\t%s %s\n",
							Consts.COLOR, valueToStr(edgeColor)));

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
