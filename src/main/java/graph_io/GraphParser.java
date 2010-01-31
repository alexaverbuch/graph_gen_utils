package graph_io;

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
	
	public abstract NodeData parseNodeAndRels(String aLine, int nodeNumber);
	public abstract NodeData parseNode(String aLine, int lineNum);
}
