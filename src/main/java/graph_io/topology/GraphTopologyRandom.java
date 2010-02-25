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

		for (int nodeNumber = 1; nodeNumber <= nodeCount; nodeNumber++) {

			NodeData node = new NodeData();

			node.getProperties().put("name", Integer.toString(nodeNumber));
			node.getProperties().put("weight", 1);

			int neighbourCount = 0;
			ArrayList<Integer> neighbours = new ArrayList<Integer>();

			while (neighbourCount < nodeDegree) {
				int neighbour = rand.nextInt(nodeCount);
				neighbour++; // Node numbering starts at 1

				if ((nodeNumber == neighbour)
						|| (neighbours.contains(neighbour)))
					continue;

				Map<String, Object> rel = new HashMap<String, Object>();
				rel.put("name", Integer.toString(neighbour));
				rel.put("weight", 1);
				node.getRelationships().add(rel);

				neighbourCount++;
				neighbours.add(neighbour);
			}

			nodes.add(node);

		}

		return nodes;

	}

}
