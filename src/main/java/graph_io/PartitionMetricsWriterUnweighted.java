package graph_io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class PartitionMetricsWriterUnweighted {

	private HashMap<Integer, Long> clusterNodes = new HashMap<Integer, Long>();
	private HashMap<Integer, Long> clusterExtDeg = new HashMap<Integer, Long>();
	private HashMap<Integer, Long> clusterIntDeg = new HashMap<Integer, Long>();
	private long nodeCount = 0;
	private long edgeCount = 0;
	private long edgeCut = 0;
	private long clusterCount = 0;
	private long meanClusterSize = 0;
	private long minClusterSize = 0;
	private long maxClusterSize = 0;
	private double modularity = 0.0;

	public void write_partition_metrics(GraphDatabaseService transNeo,
			File metricsFile) {
		clusterNodes.clear();
		clusterExtDeg.clear();
		clusterIntDeg.clear();

		nodeCount = 0;
		edgeCount = 0;
		edgeCut = 0;
		clusterCount = 0;
		meanClusterSize = 0;
		minClusterSize = 0;
		maxClusterSize = 0;
		modularity = 0.0;

		collect_graph_statistics(transNeo);
		calculate_metrics();
		metrics_to_file(metricsFile);
	}

	private void metrics_to_file(File metricsFile) {
		BufferedWriter bufferedWriter = null;
		try {
			
			bufferedWriter = new BufferedWriter(new FileWriter(metricsFile));

			bufferedWriter.write(String.format("***Graph Quality Metrics***"));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tNodes:\t\t\t%d", nodeCount));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tEdges:\t\t\t%d", edgeCount));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tEdge Cut:\t\t%d", edgeCut));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tCluster Size Variation:\t%d",
					maxClusterSize - minClusterSize));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tCluster Count:\t\t%d",
					clusterCount));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tMean Cluster Size:\t%d",
					meanClusterSize));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tMin Cluster Size:\t%d",
					minClusterSize));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tMax Cluster Size:\t%d",
					maxClusterSize));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tCluster Sizes:\t\t%s",
					clusterNodes_toString()));
			bufferedWriter.newLine();

			bufferedWriter
					.write(String.format("\tModularity:\t\t%f", modularity));
			bufferedWriter.newLine();

		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
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

	private void collect_graph_statistics(GraphDatabaseService transNeo) {

		Transaction tx = transNeo.beginTx();

		try {
			for (Node v : transNeo.getAllNodes()) {
				if (v.getId() != 0) {

					nodeCount++;

					Integer vColor = (Integer) v.getProperty("color");

					if (clusterNodes.containsKey(vColor))
						clusterNodes.put(vColor, clusterNodes.get(vColor) + 1);
					else
						clusterNodes.put(vColor, new Long(1));

					for (Relationship e : v
							.getRelationships(Direction.OUTGOING)) {

						edgeCount++;

						Node u = e.getEndNode();
						Integer uColor = (Integer) u.getProperty("color");
						if (vColor == uColor) {
							if (clusterIntDeg.containsKey(vColor))
								clusterIntDeg.put(vColor, clusterIntDeg
										.get(vColor) + 1);
							else
								clusterIntDeg.put(vColor, new Long(1));
						} else {
							if (clusterExtDeg.containsKey(vColor))
								clusterExtDeg.put(vColor, clusterExtDeg
										.get(vColor) + 1);
							else
								clusterExtDeg.put(vColor, new Long(1));
						}

					}

				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			tx.finish();
		}

		edgeCount = edgeCount / 2; // Undirected

		// Make sure all colours are represented in both HashMaps
		for (Entry<Integer, Long> intDegEntry : clusterIntDeg.entrySet()) {
			if (clusterExtDeg.containsKey(intDegEntry.getKey()) == false)
				clusterExtDeg.put(intDegEntry.getKey(), new Long(0));

			clusterIntDeg.put(intDegEntry.getKey(), intDegEntry.getValue() / 2); // Undirected
		}
		for (Entry<Integer, Long> extDegEntry : clusterExtDeg.entrySet()) {
			if (clusterIntDeg.containsKey(extDegEntry.getKey()) == false)
				clusterIntDeg.put(extDegEntry.getKey(), new Long(0));

			clusterExtDeg.put(extDegEntry.getKey(), extDegEntry.getValue() / 2); // Undirected
		}

		clusterCount = clusterIntDeg.size();
	}

	private void calculate_metrics() {
		calculate_edge_cut_metric();
		calculate_cluster_size_metrics();
		calculate_modularity();
	}

	private void calculate_edge_cut_metric() {
		for (Entry<Integer, Long> extDegEntry : clusterExtDeg.entrySet()) {
			edgeCut += extDegEntry.getValue();
		}

		edgeCut = edgeCut / 2; // Undirected
	}

	private void calculate_cluster_size_metrics() {
		minClusterSize = clusterNodes.entrySet().iterator().next().getValue();
		maxClusterSize = minClusterSize;

		for (Entry<Integer, Long> sizeEntry : clusterNodes.entrySet()) {
			long size = sizeEntry.getValue();

			meanClusterSize += size;

			if (size < minClusterSize)
				minClusterSize = size;

			if (size > maxClusterSize)
				maxClusterSize = size;
		}
		meanClusterSize = meanClusterSize / clusterNodes.size();
	}

	private void calculate_modularity() {
		for (Entry<Integer, Long> intDegEntry : clusterIntDeg.entrySet()) {
			long intDeg = intDegEntry.getValue();
			long extDeg = clusterExtDeg.get(intDegEntry.getKey());

			double left = ((double) intDeg / (double) edgeCount);
			double right = (double) (intDeg + extDeg)
					/ (2.0 * (double) edgeCount);
			modularity += left - Math.pow(right, 2);
		}
	}

	private String clusterNodes_toString() {
		String result = "";
		for (Entry<Integer, Long> clusterNodesEntry : clusterNodes.entrySet()) {
			result = String.format("%s[%d=%d]", result, clusterNodesEntry
					.getKey(), clusterNodesEntry.getValue());
		}
		return result;
	}

}
