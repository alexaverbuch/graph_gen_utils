package graph_io.topology;

import graph_io.general.NodeData;

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

		for (int nodeNumber = 0; nodeNumber < nodeCount; nodeNumber++) {

			NodeData node = new NodeData();

			node.getProperties().put("name", Integer.toString(nodeNumber));
			node.getProperties().put("weight", 1);
			node.getProperties().put("color", -1);

			for (int relNumber = 0; relNumber < nodeCount; relNumber++) {
				Map<String, Object> rel = new HashMap<String, Object>();
				rel.put("name", Integer.toString(relNumber));
				rel.put("weight", 1);
				node.getRelationships().add(rel);
			}

			nodes.add(node);

		}

		return nodes;

	}

}
