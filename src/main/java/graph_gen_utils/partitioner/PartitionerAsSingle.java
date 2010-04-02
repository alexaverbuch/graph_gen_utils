package graph_gen_utils.partitioner;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.PropNames;

import java.util.ArrayList;

public class PartitionerAsSingle implements Partitioner {

	private byte defaultPtn = -1;

	public PartitionerAsSingle(byte defaultPtn) {
		super();
		this.defaultPtn = defaultPtn;
	}

	@Override
	public ArrayList<NodeData> applyPartitioning(ArrayList<NodeData> nodes) {

		for (NodeData tempNode : nodes) {
			Byte color = new Byte(defaultPtn);
			tempNode.getProperties().put(PropNames.COLOR, color);
		}

		return nodes;

	}

}
