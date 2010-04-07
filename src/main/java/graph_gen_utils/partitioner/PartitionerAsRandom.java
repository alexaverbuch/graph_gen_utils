package graph_gen_utils.partitioner;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.Consts;

import java.util.ArrayList;
import java.util.Random;

public class PartitionerAsRandom implements Partitioner {

	private Random rand = null;
	private byte maxPtn = 0;

	public PartitionerAsRandom(byte maxPtn) {
		super();
		rand = new Random(System.currentTimeMillis());
		this.maxPtn = maxPtn;
	}

	@Override
	public ArrayList<NodeData> applyPartitioning(ArrayList<NodeData> nodes) {

		for (NodeData tempNode : nodes) {
			Byte color = (byte) rand.nextInt(maxPtn);
			tempNode.getProperties().put(Consts.COLOR, color);
		}

		return nodes;

	}

}
