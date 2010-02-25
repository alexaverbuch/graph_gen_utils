package graph_gen_utils.topology;

import graph_gen_utils.general.NodeData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GraphTopologyRandom extends GraphTopology {

	private int nodeCount = 0;
	private int nodeDegree = 0;

	public GraphTopologyRandom(int nodeCount, int nodeDegree) throws Exception {
		super();
		this.nodeCount = nodeCount;
		this.nodeDegree = nodeDegree;
		throw new Exception("Not Implemented Yet");
	}

	@Override
	public ArrayList<NodeData> getNodesAndRels() {

		HashMap<String, NodeData> nodes = new HashMap<String, NodeData>();

		Random rand = new Random(System.currentTimeMillis());

		rand.nextInt(nodeCount);

		for (int nodeID = 1; nodeID <= nodeCount; nodeID++) {

			String nodeName = Integer.toString(nodeID);

			// If node doesn't exist, create it
			if (nodes.containsKey(nodeName) == false)
				nodes.put(nodeName, new NodeData());

			NodeData node = nodes.get(nodeName);

			node.getProperties().put("name", nodeName);
			node.getProperties().put("weight", 1);

			int neighbourCount = node.NEWgetRelationshipsCollection().size();

			while (neighbourCount < nodeDegree) {
				// Node numbering starts at 1
				int neighbourID = rand.nextInt(nodeCount) + 1;

				String neighbourName = Integer.toString(neighbourID);

				// If neighbour doesn't exist, create
				if (nodes.containsKey(neighbourName) == false)
					nodes.put(neighbourName, new NodeData());

				NodeData neighbour = nodes.get(neighbourName);

				// Neighbour is self (no self loops)
				if ((nodeID == neighbourID)
				// Neighbour already in list (no multiple edges between nodes)
						|| (node.NEWgetRelationships()
								.containsKey(neighbourName)))
					// // Neighbour has max neighbours (must be symmetric)
					// || (neighbour.NEWgetRelationshipsCollection().size() >=
					// nodeDegree))
					continue;

				Map<String, Object> neighbourProps = new HashMap<String, Object>();
				neighbourProps.put("name", neighbourName);
				neighbourProps.put("weight", 1);

				// Add neighbour to my neighbour list
				node.NEWgetRelationships().put(neighbourName, neighbourProps);

				// Add me to neighbour's neighbour list (must be symmetric)
				neighbour.NEWgetRelationships().put(nodeName,
						node.getProperties());

				neighbourCount++;
			}

			nodes.put(nodeName, node);

		}

		return (ArrayList<NodeData>) nodes.values();

	}
}
