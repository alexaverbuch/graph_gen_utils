package graph_gen_utils.reader.topology;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.PropNames;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GraphTopologyRandom extends GraphTopology {

	private ArrayList<NodeData> nodesAndRels = new ArrayList<NodeData>();

	public GraphTopologyRandom(int nodeCount, int edgeCount) throws Exception {
		super();
		generateTopology(nodeCount, edgeCount);
	}

	private void generateTopology(int nodeCount, int edgeCount) {

		Random rand = new Random(System.currentTimeMillis());

		for (int nodeId = 1; nodeId <= nodeCount; nodeId++) {

			NodeData node = new NodeData();

			node.getProperties().put(PropNames.NAME, Integer.toString(nodeId));
			node.getProperties().put(PropNames.WEIGHT, 1.0);
			node.getProperties().put(PropNames.COLOR, (byte) -1);

			nodesAndRels.add(node);
		}

		int edgeNumber = 0;

		while (edgeNumber < edgeCount) {

			int nodeId1 = rand.nextInt(nodeCount);
			int nodeId2 = rand.nextInt(nodeCount);

			// No relations to self
			if (nodeId1 == nodeId2)
				continue;

			NodeData node1 = nodesAndRels.get(nodeId1);
			NodeData node2 = nodesAndRels.get(nodeId2);

			// No duplicate relations
			// NOTE May relax to only 1-directed check in future
			if ((node1.containsRelation((String) node2.getProperties().get(
					PropNames.NAME)))
					|| (node2.containsRelation((String) node1.getProperties()
							.get(PropNames.NAME))))
				continue;

			Map<String, Object> rel = new HashMap<String, Object>();
			rel.put(PropNames.NAME, node2.getProperties().get(PropNames.NAME));
			rel.put(PropNames.WEIGHT, 1.0);
			node1.getRelationships().add(rel);

			edgeNumber++;

		}

	}

	@Override
	public Iterable<NodeData> getNodes() {
		return this.nodesAndRels;
	}

	@Override
	public Iterable<NodeData> getRels() {
		return this.nodesAndRels;
	}
}
