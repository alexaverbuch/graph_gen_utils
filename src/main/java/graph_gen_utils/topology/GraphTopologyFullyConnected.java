package graph_gen_utils.topology;

import graph_gen_utils.general.NodeData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GraphTopologyFullyConnected extends GraphTopology {

	private int nodeCount = 0;

	public GraphTopologyFullyConnected(int nodeCount) {
		super();
		this.nodeCount = nodeCount;
	}

	@Override
	public ArrayList<NodeData> getNodesAndRels() {

		ArrayList<NodeData> nodes = new ArrayList<NodeData>();

		for (int nodeNumber = 1; nodeNumber <= nodeCount; nodeNumber++) {

			NodeData node = new NodeData();

			node.getProperties().put("name", Integer.toString(nodeNumber));
			node.getProperties().put("weight", 1.0);

			for (int relNumber = 1; relNumber <= nodeCount; relNumber++) {
				if (nodeNumber == relNumber)
					continue;
				
				Map<String, Object> rel = new HashMap<String, Object>();
				rel.put("name", Integer.toString(relNumber));
				rel.put("weight", 1.0);
				node.getRelationships().add(rel);
			}

			nodes.add(node);

		}

		return nodes;

	}

}
