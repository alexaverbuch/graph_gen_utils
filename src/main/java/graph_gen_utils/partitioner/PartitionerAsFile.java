package graph_gen_utils.partitioner;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.PropNames;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class PartitionerAsFile implements Partitioner {

	private Scanner partitionScanner = null;

	public PartitionerAsFile(File partitionFile) throws FileNotFoundException {
		super();
		partitionScanner = new Scanner(partitionFile);
	}

	@Override
	public ArrayList<NodeData> applyPartitioning(ArrayList<NodeData> nodes) {

		for (NodeData tempNode : nodes) {
			Byte color = Byte.parseByte(partitionScanner.nextLine());
			tempNode.getProperties().put(PropNames.COLOR, color);
		}

		return nodes;

	}

}
