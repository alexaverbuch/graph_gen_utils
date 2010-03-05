package graph_gen_utils.chaco;

import graph_gen_utils.general.NodeData;

public abstract class ChacoParser {
	private int nodeCount;
	private int edgeCount;	
	
	public ChacoParser(int nodeCount, int edgeCount) {
		super();
		this.nodeCount = nodeCount;
		this.edgeCount = edgeCount;
	}

	public int getNodeCount() {
		return nodeCount;
	}

	public int getEdgeCount() {
		return edgeCount;
	}				
	
	public abstract NodeData parseNodeAndRels(String aLine, int nodeNumber);
	public abstract NodeData parseNode(String aLine, int lineNum);
}
