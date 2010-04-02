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

public class ChacoWriterUnweighted implements GraphWriter {

	private File chacoFile = null;

	public ChacoWriterUnweighted(File chacoFile) {
		this.chacoFile = chacoFile;
	}

	@Override
	public void write(GraphDatabaseService transNeo) {

		BufferedWriter bufferedWriter = null;

		Transaction tx = transNeo.beginTx();

		try {
			bufferedWriter = new BufferedWriter(new FileWriter(chacoFile));

			long nodeCount = 0;
			long edgeCount = 0;
			for (Node node : transNeo.getAllNodes()) {

				nodeCount++;

				// Only consider OUTGOING so edges counted once
				for (Relationship rel : node
						.getRelationships(Direction.OUTGOING)) {
					edgeCount++;
				}
			}

			String firstLine = String.format("%d %d 0", nodeCount,
					edgeCount * 2);
			bufferedWriter.write(firstLine);

			for (Node node : transNeo.getAllNodes()) {

				bufferedWriter.newLine();

				// Chaco files assumed to be undirected. Edges are bidirectional
				for (Relationship rel : node.getRelationships(Direction.BOTH)) {

					String name = (String) rel.getOtherNode(node).getProperty(
							PropNames.NAME);
					bufferedWriter.write(" " + name);

				}

			}

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
