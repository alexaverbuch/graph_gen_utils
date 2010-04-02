package graph_gen_utils.partitioner;

import java.util.ArrayList;

import graph_gen_utils.general.NodeData;

public interface Partitioner {

	public ArrayList<NodeData> applyPartitioning(ArrayList<NodeData> nodes);
}
