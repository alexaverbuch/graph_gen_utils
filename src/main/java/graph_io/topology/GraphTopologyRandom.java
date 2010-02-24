package graph_io.topology;

import graph_io.general.NodeData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GraphTopologyRandom extends GraphTopology {

	private int nodeCount = 0;
	private int nodeDegree = 0;

	public GraphTopologyRandom(int nodeCount, int nodeDegree) {
		super();
		this.nodeCount = nodeCount;
		this.nodeDegree = nodeDegree;
	}

	@Override
	public ArrayList<NodeData> getNodesAndRels() {

		ArrayList<NodeData> nodes = new ArrayList<NodeData>();

		Random rand = new Random(System.currentTimeMillis());

		rand.nextInt(nodeCount);

		for (int nodeNumber = 0; nodeNumber < nodeCount; nodeNumber++) {

			NodeData node = new NodeData();

			node.getProperties().put("name", Integer.toString(nodeNumber));
			node.getProperties().put("weight", 1);
			node.getProperties().put("color", -1);

			for (int relNumber = 0; relNumber < nodeDegree; relNumber++) {
				Map<String, Object> rel = new HashMap<String, Object>();
				rel.put("name", Integer.toString(rand.nextInt(nodeCount)));
				rel.put("weight", 1);
				node.getRelationships().add(rel);
			}

			nodes.add(node);

		}

		return nodes;

	}

}
