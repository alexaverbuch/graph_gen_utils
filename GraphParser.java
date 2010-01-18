// Author: Alex Averbuch

package graph_gen_utils;

public abstract class GraphParser {
	private int nodeCount;
	private int edgeCount;	
	
	public GraphParser(int nodeCount, int edgeCount) {
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
	
	public abstract NodeData parseLine(String aLine, int lineNum);

}
