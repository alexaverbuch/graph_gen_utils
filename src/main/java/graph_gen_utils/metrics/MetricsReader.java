package graph_gen_utils.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class MetricsReader {

	public enum MetricsType {
		ALL, GRAPH, CLUSTERING
	}

	private HashMap<Byte, Long> clusterNodes = new HashMap<Byte, Long>();
	private HashMap<Byte, Long> clusterExtDeg = new HashMap<Byte, Long>();
	private HashMap<Byte, Long> clusterIntDeg = new HashMap<Byte, Long>();
	private long nodeCount = 0;
	private long edgeCount = 0;
	private long edgeCut = 0;
	private long clusterCount = 0;
	private long meanClusterSize = 0;
	private long minClusterSize = 0;
	private long maxClusterSize = 0;
	private double modularity = 0.0;
	private double clusteringCoefficient = 0.0;
	private GraphDatabaseService transNeo = null;

	public MetricsReader(GraphDatabaseService transNeo, MetricsType metricsType) {
		this.transNeo = transNeo;

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

		switch (metricsType) {
		case ALL:
			calcMetricsAll();
			break;
		case GRAPH:
			calcMetricsGraph();
			break;
		case CLUSTERING:
			calcMetricsClustering();
			break;
		}

	}

	public long nodeCount() {
		return nodeCount;
	}

	public long edgeCount() {
		return edgeCount;
	}

	public long edgeCut() {
		return edgeCut;
	}

	public double edgeCutPerc() {
		return (double) edgeCut / (double) edgeCount;
	}

	public long clusterSizeDiff() {
		return maxClusterSize - minClusterSize;
	}

	public double clusterSizeDiffPerc() {
		return (double) clusterSizeDiff() / (double) nodeCount;
	}

	public long clusterCount() {
		return clusterCount;
	}

	public long meanClusterSize() {
		return meanClusterSize;
	}

	public long minClusterSize() {
		return minClusterSize;
	}

	public long maxClusterSize() {
		return maxClusterSize;
	}

	public double modularity() {
		return modularity;
	}

	public double clusteringCoefficient() {
		return clusteringCoefficient;
	}

	public String clustersToString() {
		String result = "";
		for (Entry<Byte, Long> clusterNodesEntry : clusterNodes.entrySet()) {
			result = String.format("%s[%d=%d]", result, clusterNodesEntry
					.getKey(), clusterNodesEntry.getValue());
		}
		return result;
	}

	private void calcMetricsAll() {

		Transaction tx = transNeo.beginTx();

		try {
			for (Node v : transNeo.getAllNodes()) {
				if (v.getId() != 0) {

					nodeCount++;

					Byte vColor = (Byte) v.getProperty("color");

					if (clusterNodes.containsKey(vColor))
						clusterNodes.put(vColor, clusterNodes.get(vColor) + 1);
					else
						clusterNodes.put(vColor, new Long(1));

					// Used for Clustering Coefficient
					double nodeDegree = edgeCount;
					double nodeNeighbourRels = 0.0;
					ArrayList<Node> nodeNeighbours = new ArrayList<Node>();
					ArrayList<Long> nodeNeighboursIDs = new ArrayList<Long>();

					for (Relationship e : v
							.getRelationships(Direction.OUTGOING)) {

						edgeCount++;

						Node u = e.getEndNode();
						Byte uColor = (Byte) u.getProperty("color");
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

						// Used for Clustering Coefficient
						nodeNeighbours.add(u);
						nodeNeighboursIDs.add(u.getId());

					}

					// Used for Clustering Coefficient
					nodeDegree = edgeCount - nodeDegree;

					// Used for Clustering Coefficient
					for (Node nodeNeighbour : nodeNeighbours) {
						for (Relationship e : nodeNeighbour
								.getRelationships(Direction.OUTGOING)) {

							Node nodeNeighboursNeighbour = e.getEndNode();

							// my neighbour neighbours my other neighbours
							if (nodeNeighboursIDs
									.contains(nodeNeighboursNeighbour.getId()))
								nodeNeighbourRels++;

						}
					}

					// Add local clustering coefficient to global clustering
					// coefficient
					double denominator = nodeDegree * (nodeDegree - 1);
					if (denominator != 0)
						clusteringCoefficient += (nodeNeighbourRels / (nodeDegree * (nodeDegree - 1)));

				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			tx.finish();
		}

		clusteringCoefficient = clusteringCoefficient / (double) nodeCount;

		edgeCount = edgeCount / 2; // Undirected

		// Make sure all colours are represented in both HashMaps
		for (Entry<Byte, Long> extDegEntry : clusterExtDeg.entrySet())
			if (clusterIntDeg.containsKey(extDegEntry.getKey()) == false)
				clusterIntDeg.put(extDegEntry.getKey(), new Long(0));
		for (Entry<Byte, Long> intDegEntry : clusterIntDeg.entrySet()) {
			if (clusterExtDeg.containsKey(intDegEntry.getKey()) == false)
				clusterExtDeg.put(intDegEntry.getKey(), new Long(0));
			// clusterIntDeg.put(intDegEntry.getKey(), intDegEntry.getValue() /
			// 2);
		}

		clusterCount = clusterIntDeg.size();

		calcEdgCutMetric();
		calcClusterSizeMetrics();
		calcModularity();
	}

	private void calcMetricsGraph() {

		Transaction tx = transNeo.beginTx();

		try {
			for (Node v : transNeo.getAllNodes()) {
				if (v.getId() != 0) {

					nodeCount++;

					Byte vColor = (Byte) v.getProperty("color");

					// Used for Clustering Coefficient
					double nodeDegree = edgeCount;
					double nodeNeighbourRels = 0.0;
					ArrayList<Node> nodeNeighbours = new ArrayList<Node>();

					for (Relationship e : v
							.getRelationships(Direction.OUTGOING)) {

						edgeCount++;

						Node u = e.getEndNode();

						// Used for Clustering Coefficient
						nodeNeighbours.add(u);

					}

					// Used for Clustering Coefficient
					nodeDegree = edgeCount - nodeDegree;

					// Used for Clustering Coefficient
					for (Node nodeNeighbour : nodeNeighbours) {
						for (Relationship e : nodeNeighbour
								.getRelationships(Direction.OUTGOING)) {

							Node nodeNeighboursNeighbour = e.getEndNode();

							// my neighbour neighbours my other neighbours
							if (nodeNeighbours
									.contains(nodeNeighboursNeighbour))
								nodeNeighbourRels++;

						}
					}

					// Add local clustering coefficient to global clustering
					// coefficient
					clusteringCoefficient += nodeNeighbourRels
							/ (nodeDegree * (nodeDegree - 1));

				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			tx.finish();
		}

		clusteringCoefficient = clusteringCoefficient / (double) nodeCount;

		edgeCount = edgeCount / 2; // Undirected
	}

	private void calcMetricsClustering() {

		Transaction tx = transNeo.beginTx();

		try {
			for (Node v : transNeo.getAllNodes()) {
				if (v.getId() != 0) {

					nodeCount++;

					Byte vColor = (Byte) v.getProperty("color");

					if (clusterNodes.containsKey(vColor))
						clusterNodes.put(vColor, clusterNodes.get(vColor) + 1);
					else
						clusterNodes.put(vColor, new Long(1));

					for (Relationship e : v
							.getRelationships(Direction.OUTGOING)) {

						edgeCount++;

						Node u = e.getEndNode();
						Byte uColor = (Byte) u.getProperty("color");
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
		for (Entry<Byte, Long> extDegEntry : clusterExtDeg.entrySet())
			if (clusterIntDeg.containsKey(extDegEntry.getKey()) == false)
				clusterIntDeg.put(extDegEntry.getKey(), new Long(0));
		for (Entry<Byte, Long> intDegEntry : clusterIntDeg.entrySet()) {
			if (clusterExtDeg.containsKey(intDegEntry.getKey()) == false)
				clusterExtDeg.put(intDegEntry.getKey(), new Long(0));
			// clusterIntDeg.put(intDegEntry.getKey(), intDegEntry.getValue() /
			// 2);
		}

		clusterCount = clusterIntDeg.size();

		calcEdgCutMetric();
		calcClusterSizeMetrics();
		calcModularity();
	}

	private void calcEdgCutMetric() {
		edgeCut = 0;
		for (Entry<Byte, Long> extDeg : clusterExtDeg.entrySet()) {
			edgeCut += extDeg.getValue();
		}

		edgeCut = edgeCut / 2; // Undirected
	}

	private void calcClusterSizeMetrics() {
		minClusterSize = clusterNodes.entrySet().iterator().next().getValue();
		maxClusterSize = minClusterSize;

		for (Entry<Byte, Long> nodes : clusterNodes.entrySet()) {
			long size = nodes.getValue();

			meanClusterSize += size;

			if (size < minClusterSize)
				minClusterSize = size;

			if (size > maxClusterSize)
				maxClusterSize = size;
		}
		meanClusterSize = meanClusterSize / clusterNodes.size();
	}

	private void calcModularity() {
		for (Entry<Byte, Long> intDegEntry : clusterIntDeg.entrySet()) {
			long intDeg = intDegEntry.getValue();
			long extDeg = clusterExtDeg.get(intDegEntry.getKey());

			double left = ((double) intDeg / (double) edgeCount);
			double right = (double) (intDeg + extDeg)
					/ (2.0 * (double) edgeCount);
			modularity += left - Math.pow(right, 2);
		}
	}

}
