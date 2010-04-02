package graph_gen_utils.writer.chaco;

import graph_gen_utils.general.PropNames;
import graph_gen_utils.writer.GraphWriter;

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

public class ChacoPtnWriterUnweighted implements GraphWriter {

	private File chacoFile = null;
	private File ptnFile = null;

	public ChacoPtnWriterUnweighted(File chacoFile, File ptnFile) {
		this.chacoFile = chacoFile;
		this.ptnFile = ptnFile;
	}

	@Override
	public void write(GraphDatabaseService transNeo) {

		BufferedWriter bufferedChacoWriter = null;
		BufferedWriter bufferedPtnWriter = null;

		Transaction tx = transNeo.beginTx();

		try {
			bufferedChacoWriter = new BufferedWriter(new FileWriter(chacoFile));
			bufferedPtnWriter = new BufferedWriter(new FileWriter(ptnFile));

			long nodeCount = 0;
			long edgeCount = 0;
			for (Node node : transNeo.getAllNodes()) {

				nodeCount++;

				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {
					edgeCount++;
				}
			}

			String firstLine = String.format("%d %d 0", nodeCount,
					edgeCount * 2);
			bufferedChacoWriter.write(firstLine);

			boolean firstPtnLine = true;

			for (Node node : transNeo.getAllNodes()) {

				bufferedChacoWriter.newLine();

				// Chaco files assumed to be undirected. Edges are bidirectional
				for (Relationship rel : node.getRelationships(Direction.BOTH)) {

					String name = (String) rel.getOtherNode(node).getProperty(
							PropNames.NAME);
					bufferedChacoWriter.write(" " + name);

				}

				if (!firstPtnLine)
					bufferedPtnWriter.newLine();
				else
					firstPtnLine = false;

				String line = "-1";
				if (node.hasProperty(PropNames.COLOR)) {
					Byte color = (Byte) node.getProperty(PropNames.COLOR);
					line = color.toString();
				}

				bufferedPtnWriter.write(line);

			}

		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {

			tx.finish();

			// Close the BufferedWriters
			try {
				if (bufferedChacoWriter != null) {
					bufferedChacoWriter.flush();
					bufferedChacoWriter.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}

			// Close the BufferedWriter
			try {
				if (bufferedPtnWriter != null) {
					bufferedPtnWriter.flush();
					bufferedPtnWriter.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}

}
