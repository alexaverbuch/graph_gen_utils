package graph_gen_utils.partitioner;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.PropNames;

import java.util.ArrayList;

public class PartitionerAsBalanced implements Partitioner {

	private byte lastPtn = -1;
	private byte maxPtn = 0;

	public PartitionerAsBalanced(byte maxPtn) {
		super();
		this.maxPtn = maxPtn;
	}

	@Override
	public ArrayList<NodeData> applyPartitioning(ArrayList<NodeData> nodes) {

		for (NodeData tempNode : nodes) {
			lastPtn++;
			if (lastPtn >= maxPtn)
				lastPtn = 0;
			Byte color = lastPtn;
			tempNode.getProperties().put(PropNames.COLOR, color);
		}

		return nodes;

	}

}
