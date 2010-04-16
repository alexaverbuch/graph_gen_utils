package graph_gen_utils.partitioner;

import graph_gen_utils.general.NodeData;

import java.util.ArrayList;

public class PartitionerAsDefault implements Partitioner {

	public PartitionerAsDefault() {
		super();
	}

	@Override
	public ArrayList<NodeData> applyPartitioning(ArrayList<NodeData> nodes) {

		return nodes;

	}

}
