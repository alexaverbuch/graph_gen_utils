package graph_gen_utils.topology;

import graph_gen_utils.general.NodeData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GraphTopologyRandom extends GraphTopology {

	private int nodeCount = 0;
	private int edgeCount = 0;

	public GraphTopologyRandom(int nodeCount, int edgeCount) throws Exception {
		super();
		this.nodeCount = nodeCount;
		this.edgeCount = edgeCount;
	}

	@Override
	public ArrayList<NodeData> getNodesAndRels() {

		ArrayList<NodeData> nodes = new ArrayList<NodeData>();

		Random rand = new Random(System.currentTimeMillis());

		for (int nodeId = 1; nodeId <= nodeCount; nodeId++) {

			NodeData node = new NodeData();

			node.getProperties().put("name", Integer.toString(nodeId));
			node.getProperties().put("weight", 1.0);

			nodes.add(node);
		}

		int edgeNumber = 0;

		while (edgeNumber < edgeCount) {

			int nodeId1 = rand.nextInt(nodeCount);
			int nodeId2 = rand.nextInt(nodeCount);

			// No relations to self
			if (nodeId1 == nodeId2)
				continue;

			NodeData node1 = nodes.get(nodeId1);
			NodeData node2 = nodes.get(nodeId2);

			// No duplicate relations
			if ((node1.containsRelation((String) node2.getProperties().get(
					"name")))
					|| (node2.containsRelation((String) node1.getProperties()
							.get("name"))))
				continue;

			Map<String, Object> rel1 = new HashMap<String, Object>();
			rel1.put("name", node2.getProperties().get("name"));
			rel1.put("weight", 1.0);
			node1.getRelationships().add(rel1);

			Map<String, Object> rel2 = new HashMap<String, Object>();
			rel2.put("name", node1.getProperties().get("name"));
			rel2.put("weight", 1.0);
			node2.getRelationships().add(rel2);

			edgeNumber++;

		}

		return nodes;

	}
}
