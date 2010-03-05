package graph_gen_utils.chaco;

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

	@Override
	public void write(GraphDatabaseService transNeo, File chacoFile,
			File ptnFile) {

		BufferedWriter bufferedChacoWriter = null;
		BufferedWriter bufferedPtnWriter = null;

		Transaction tx = transNeo.beginTx();

		try {
			bufferedChacoWriter = new BufferedWriter(new FileWriter(chacoFile));
			bufferedPtnWriter = new BufferedWriter(new FileWriter(ptnFile));

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
			bufferedChacoWriter.write(firstLine);

			boolean firstPtnLine = true;

			for (Node node : transNeo.getAllNodes()) {
				if (node.getId() != 0) {

					bufferedChacoWriter.newLine();

					for (Relationship rel : node
							.getRelationships(Direction.OUTGOING)) {
						String name = (String) rel.getEndNode().getProperty(
								"name");
						bufferedChacoWriter.write(" " + name);
					}

					if (!firstPtnLine)
						bufferedPtnWriter.newLine();
					else
						firstPtnLine = false;

					String line = "0";
					if (node.hasProperty("color")) {
						Integer color = (Integer) node.getProperty("color");
						line = color.toString();
					}

					bufferedPtnWriter.write(line);
				}
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
