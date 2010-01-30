package graph_gen_utils;

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

public class ChacoWriterUnweighted extends ChacoWriter {

	@Override
	public void write(GraphDatabaseService transNeo, File chacoFile) {

		BufferedWriter bufferedWriter = null;

		Transaction tx = transNeo.beginTx();

		try {
			bufferedWriter = new BufferedWriter(new FileWriter(chacoFile));

			long nodeCount = 0;
			long edgeCount = 0;
			for (Node node : transNeo.getAllNodes()) {
				if (node.getId() != 0) {
					nodeCount++;

					for (Relationship rel : node
							.getRelationships(Direction.OUTGOING)) {
						edgeCount++;
					}
				}
			}
			
			String firstLine = String.format("%d %d 0", nodeCount, edgeCount);
			bufferedWriter.write(firstLine);

			for (Node node : transNeo.getAllNodes()) {
				if (node.getId() != 0) {
					bufferedWriter.newLine();

					for (Relationship rel : node
							.getRelationships(Direction.OUTGOING)) {
						String name = (String) rel.getEndNode().getProperty(
								"name");
						bufferedWriter.write(" " + name);
					}
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
