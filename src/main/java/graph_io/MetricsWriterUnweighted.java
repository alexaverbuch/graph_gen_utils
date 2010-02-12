package graph_io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;

public class MetricsWriterUnweighted {

	public static void writeMetrics(GraphDatabaseService transNeo,
			File metricsFile) {

		MetricsReader metricsReader = new MetricsReader(transNeo);

		BufferedWriter bufferedWriter = null;
		try {

			bufferedWriter = new BufferedWriter(new FileWriter(metricsFile));

			bufferedWriter.write(String.format("***Graph Quality Metrics***"));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tNodes:\t\t\t%d",
					metricsReader.nodeCount()));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tEdges:\t\t\t%d",
					metricsReader.edgeCount()));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tEdge Cut:\t\t%d:",
					metricsReader.edgeCut()));
			bufferedWriter.write(String.format("\t\t%f", metricsReader
					.edgeCutPerc()));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format(
					"\tCluster Size Variation:\t%d:", metricsReader
							.clusterSizeDiff()));
			bufferedWriter.write(String.format("\t\t%f", metricsReader
					.clusterSizeDiffPerc()));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tCluster Count:\t\t%d",
					metricsReader.clusterCount()));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tMean Cluster Size:\t%d",
					metricsReader.meanClusterSize()));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tMin Cluster Size:\t%d",
					metricsReader.minClusterSize()));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tMax Cluster Size:\t%d",
					metricsReader.maxClusterSize()));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tCluster Sizes:\t\t%s",
					metricsReader.clustersToString()));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format("\tModularity:\t\t%f",
					metricsReader.modularity()));
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

	public static void writeMetricsCSV(GraphDatabaseService transNeo,
			File metricsFile) {

		MetricsReader metricsReader = new MetricsReader(transNeo);

		BufferedWriter bufferedWriter = null;
		try {

			bufferedWriter = new BufferedWriter(new FileWriter(metricsFile));

			bufferedWriter
					.write(String
							.format("Nodes,Edges,EdgeCut,EdgeCutPerc,ClusterCount,ClusterSizeDiff,ClusterSizeDiffPerc"));
			bufferedWriter
					.write(String
							.format("MeanClusterSize,MinClusterSize,MaxClusterCount,Modularity,Clusters"));
			bufferedWriter.newLine();

			bufferedWriter.write(String.format(
					"%d,%d,%d,%f,%d,%d,%f,%d,%d,%d,%f,%s", metricsReader
							.nodeCount(), metricsReader.edgeCount(),
					metricsReader.edgeCut(), metricsReader.edgeCutPerc(),
					metricsReader.clusterCount(), metricsReader
							.clusterSizeDiff(), metricsReader
							.clusterSizeDiffPerc(), metricsReader
							.meanClusterSize(), metricsReader.minClusterSize(),
					metricsReader.maxClusterSize(), metricsReader.modularity(),
					metricsReader.clustersToString()));
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

	public static void appendMetricsCSV(GraphDatabaseService transNeo,
			File metricsFile) {

		MetricsReader metricsReader = new MetricsReader(transNeo);

		BufferedWriter bufferedWriter = null;
		try {

			bufferedWriter = new BufferedWriter(new FileWriter(metricsFile,
					true));

			bufferedWriter.write(String.format(
					"%d,%d,%d,%f,%d,%d,%f,%d,%d,%d,%f,%s", metricsReader
							.nodeCount(), metricsReader.edgeCount(),
					metricsReader.edgeCut(), metricsReader.edgeCutPerc(),
					metricsReader.clusterCount(), metricsReader
							.clusterSizeDiff(), metricsReader
							.clusterSizeDiffPerc(), metricsReader
							.meanClusterSize(), metricsReader.minClusterSize(),
					metricsReader.maxClusterSize(), metricsReader.modularity(),
					metricsReader.clustersToString()));
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

}
