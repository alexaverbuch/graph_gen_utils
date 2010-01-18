package graph_gen_utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

public class GraphParserUnweighted extends GraphParser {

	public GraphParserUnweighted(int nodeCount, int edgeCount) {
		super(nodeCount, edgeCount);
	}

	@Override
	public NodeData parseLine(String aLine, int lineNum) {
		try {
			NodeData node = new NodeData();

			// Scanner scanner = new Scanner(aLine.trim());
			// scanner.useDelimiter(" ");
			StringTokenizer st = new StringTokenizer(aLine, " ");

			node.getProperties().put("name", Integer.toString(lineNum));
			node.getProperties().put("weight", 1);

			// while (scanner.hasNext()) {
			while (st.hasMoreTokens()) {
				Map<String, Object> rel = new HashMap<String, Object>();
				rel.put("name", st.nextToken());
				rel.put("weight", 1);
				node.getRelationships().add(rel);
			}

//			scanner.close();
			return node;
		} catch (Exception e) {
			System.err.printf("Could not parse line %d%n%n%s", lineNum + 1, e
					.toString());
			return null;
		}
	}

}
