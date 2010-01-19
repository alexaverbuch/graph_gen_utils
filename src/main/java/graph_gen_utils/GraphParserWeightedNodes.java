package graph_gen_utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

public class GraphParserWeightedNodes extends GraphParser {

	public GraphParserWeightedNodes(int nodeCount, int edgeCount) {
		super(nodeCount, edgeCount);
	}

	@Override
	public NodeData parseNodeAndRels(String aLine, int nodeNumber) {
		try {
			NodeData node = new NodeData();
			
			StringTokenizer st = new StringTokenizer(aLine, " ");
			
			node.getProperties().put("name", Integer.toString(nodeNumber));
			node.getProperties().put("weight", st.nextToken());
			
			while (st.hasMoreTokens()) {
				Map<String,Object> rel = new HashMap<String, Object>();			
				rel.put("name", st.nextToken());
				rel.put("weight", 1);
				node.getRelationships().add(rel);
			}
			
			return node;
		} catch (Exception e) {
			System.err.printf("Could not parse line %d%n%n%s", nodeNumber+1, e
					.toString());
			return null;
		}
	}

	@Override
	public NodeData parseNode(String aLine, int nodeNumber) {
		try {
			NodeData node = new NodeData();
			
			StringTokenizer st = new StringTokenizer(aLine, " ");
			
			node.getProperties().put("name", Integer.toString(nodeNumber));
			node.getProperties().put("weight", st.nextToken());
			
			return node;
		} catch (Exception e) {
			System.err.printf("Could not parse line %d%n%n%s", nodeNumber+1, e
					.toString());
			return null;
		}
	}

}
