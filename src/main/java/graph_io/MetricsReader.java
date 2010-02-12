package graph_io;

import java.util.HashMap;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class MetricsReader {

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
	private GraphDatabaseService transNeo = null;

	public MetricsReader(GraphDatabaseService transNeo) {
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

		calcMetrics();
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

	public String clustersToString() {
		String result = "";
		for (Entry<Integer, Long> clusterNodesEntry : clusterNodes.entrySet()) {
			result = String.format("%s[%d=%d]", result, clusterNodesEntry
					.getKey(), clusterNodesEntry.getValue());
		}
		return result;
	}

	private void calcMetrics() {

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
		for (Entry<Integer, Long> extDegEntry : clusterExtDeg.entrySet())
			if (clusterIntDeg.containsKey(extDegEntry.getKey()) == false)
				clusterIntDeg.put(extDegEntry.getKey(), new Long(0));
		for (Entry<Integer, Long> intDegEntry : clusterIntDeg.entrySet()) {
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
		for (Entry<Integer, Long> extDeg : clusterExtDeg.entrySet()) {
			edgeCut += extDeg.getValue();
		}

		edgeCut = edgeCut / 2; // Undirected
	}

	private void calcClusterSizeMetrics() {
		minClusterSize = clusterNodes.entrySet().iterator().next().getValue();
		maxClusterSize = minClusterSize;

		for (Entry<Integer, Long> nodes : clusterNodes.entrySet()) {
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
		for (Entry<Integer, Long> intDegEntry : clusterIntDeg.entrySet()) {
			long intDeg = intDegEntry.getValue();
			long extDeg = clusterExtDeg.get(intDegEntry.getKey());

			double left = ((double) intDeg / (double) edgeCount);
			double right = (double) (intDeg + extDeg)
					/ (2.0 * (double) edgeCount);
			modularity += left - Math.pow(right, 2);
		}
	}

}
