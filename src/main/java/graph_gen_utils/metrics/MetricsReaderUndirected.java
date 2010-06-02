package graph_gen_utils.metrics;

import graph_gen_utils.general.Consts;
import graph_gen_utils.general.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class MetricsReaderUndirected implements MetricsReader {

	// Number of nodes in each cluster
	private HashMap<Byte, Long> clusterNodes = new HashMap<Byte, Long>();

	// Number of edges leaving each cluster
	private HashMap<Byte, Long> clusterExtDeg = new HashMap<Byte, Long>();

	// Number of edges inside (between nodes of same cluster) each cluster
	private HashMap<Byte, Long> clusterIntDeg = new HashMap<Byte, Long>();

	// Number of nodes in entire graph
	private long nodeCount = 0;

	// Number of edges in entire graph
	private long edgeCount = 0;

	// Number of edges in entire graph that connect nodes of different clusters
	private long edgeCut = 0;

	// Number of clusters in entire graph
	private long clusterCount = 0;

	// Mean number of nodes in a cluster
	private long meanClusterSize = 0;

	// Minimum number of nodes in a cluster
	private long minClusterSize = 0;

	// Maximum number of nodes in a cluster
	private long maxClusterSize = 0;

	// Over simplified version:
	// Ratio between edges inside clusters
	// AND
	// Expected number of edges outside clusters, assuming a random topology
	private double modularity = 0.0;

	// Over simplified version:
	// Ratio representing how many of a nodes neighbours also connect to its
	// other neighbours
	private double clusteringCoefficient = 0.0;

	private GraphDatabaseService transNeo = null;

	public MetricsReaderUndirected(GraphDatabaseService transNeo) {
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

		collectDataAndCalcMetrics();

	}

	// --------------
	// Getter Methods
	// --------------

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

	// -------------
	// Metrics Calcs
	// -------------

	private void collectDataAndCalcMetrics() {

		Transaction tx = transNeo.beginTx();

		try {
			for (Node v : transNeo.getAllNodes()) {

				nodeCount++;

				Byte vColor = (Byte) v.getProperty(Consts.COLOR);

				if (clusterNodes.containsKey(vColor))
					clusterNodes.put(vColor, clusterNodes.get(vColor) + 1);
				else
					clusterNodes.put(vColor, new Long(1));

				// FIXME Uncomment Later!
				// // Used for Clustering Coefficient
				// double nodeDegree = 0.0;
				// double nodeNeighbourRels = 0.0;
				// ArrayList<Node> nodeNeighbours = new ArrayList<Node>();
				// ArrayList<Long> nodeNeighboursIDs = new ArrayList<Long>();

				for (Relationship e : v.getRelationships(Direction.BOTH)) {

					edgeCount++;

					// FIXME Uncomment Later!
					// // Used for Clustering Coefficient
					// nodeDegree++;

					Node u = e.getOtherNode(v);
					Byte uColor = (Byte) u.getProperty(Consts.COLOR);

					if (vColor == uColor) {
						if (clusterIntDeg.containsKey(vColor))
							clusterIntDeg.put(vColor,
									clusterIntDeg.get(vColor) + 1);
						else
							clusterIntDeg.put(vColor, new Long(1));
					} else {
						if (clusterExtDeg.containsKey(vColor))
							clusterExtDeg.put(vColor,
									clusterExtDeg.get(vColor) + 1);
						else
							clusterExtDeg.put(vColor, new Long(1));
					}

					// FIXME Uncomment Later!
					// // Used for Clustering Coefficient
					// nodeNeighbours.add(u);
					// nodeNeighboursIDs.add(u.getId());

				}

				// FIXME Uncomment Later!
				// // Used for Clustering Coefficient
				// for (Node nodeNeighbour : nodeNeighbours) {
				// for (Relationship e : nodeNeighbour
				// .getRelationships(Direction.BOTH)) {
				//
				// Node nodeNeighboursNeighbour = e
				// .getOtherNode(nodeNeighbour);
				//
				// // my neighbour neighbours my other neighbours
				// if (nodeNeighboursIDs.contains(nodeNeighboursNeighbour
				// .getId()))
				// nodeNeighbourRels++;
				//
				// }
				// }

				// FIXME Uncomment Later!
				// // Add local clustering coefficient to global clustering
				// // coefficient
				// double denominator = nodeDegree * (nodeDegree - 1);
				// if (denominator != 0)
				// clusteringCoefficient += (nodeNeighbourRels / denominator);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			tx.finish();
		}

		// FIXME Uncomment Later!
		// clusteringCoefficient = clusteringCoefficient / (double) nodeCount;

		edgeCount = edgeCount / 2; // Undirected

		// Make sure all colours are represented in both HashMaps
		for (Byte extDegKey : clusterExtDeg.keySet())
			if (clusterIntDeg.containsKey(extDegKey) == false)
				clusterIntDeg.put(extDegKey, new Long(0));

		// Make sure all colours are represented in both HashMaps
		for (Byte intDegKey : clusterIntDeg.keySet()) {
			if (clusterExtDeg.containsKey(intDegKey) == false)
				clusterExtDeg.put(intDegKey, new Long(0));
		}

		clusterCount = clusterIntDeg.size();

		calcEdgCutMetric();
		calcClusterSizeMetrics();
		calcModularity();
	}

	private void calcEdgCutMetric() {
		edgeCut = 0;
		for (Entry<Byte, Long> extDeg : clusterExtDeg.entrySet())
			edgeCut += extDeg.getValue();

		edgeCut = edgeCut / 2; // Undirected
	}

	private void calcClusterSizeMetrics() {
		if (clusterNodes.entrySet().iterator().hasNext() == false) {
			minClusterSize = 0;
			maxClusterSize = 0;
			meanClusterSize = 0;
			return;
		}

		minClusterSize = clusterNodes.entrySet().iterator().next().getValue();
		maxClusterSize = minClusterSize;

		for (Entry<Byte, Long> nodesInCluster : clusterNodes.entrySet()) {
			long size = nodesInCluster.getValue();

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
