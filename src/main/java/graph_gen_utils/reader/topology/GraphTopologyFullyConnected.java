package graph_gen_utils.reader.topology;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.PropNames;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class GraphTopologyFullyConnected extends GraphTopology {

	private ArrayList<NodeData> nodesAndRels = new ArrayList<NodeData>();

	public GraphTopologyFullyConnected(int nodeCount)
			throws FileNotFoundException {
		generateTopology(nodeCount);
	}

	private void generateTopology(int nodeCount) {

		for (int nodeNumber = 1; nodeNumber <= nodeCount; nodeNumber++) {

			NodeData node = new NodeData();

			node.getProperties().put(PropNames.NAME, Integer.toString(nodeNumber));
			node.getProperties().put(PropNames.WEIGHT, 1.0);
			node.getProperties().put(PropNames.COLOR, (byte) -1);

			for (int relNumber = 1; relNumber <= nodeCount; relNumber++) {
				if (relNumber <= nodeNumber)
					continue;

				Map<String, Object> rel = new HashMap<String, Object>();
				rel.put(PropNames.NAME, Integer.toString(relNumber));
				rel.put(PropNames.WEIGHT, 1.0);
				node.getRelationships().add(rel);
			}

			nodesAndRels.add(node);

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
