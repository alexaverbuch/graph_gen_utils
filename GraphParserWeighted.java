// Author: Alex Averbuch

package graph_gen_utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

public class GraphParserWeighted extends GraphParser {

	public GraphParserWeighted(int nodeCount, int edgeCount) {
		super(nodeCount, edgeCount);
	}

	@Override
	public NodeData parseLine(String aLine, int lineNum) {
		try {
			NodeData node = new NodeData();

			StringTokenizer st = new StringTokenizer(aLine, " ");
//			Scanner scanner = new Scanner(aLine.trim());
//			scanner.useDelimiter(" ");

			node.getProperties().put("name", Integer.toString(lineNum));
			node.getProperties().put("weight", st.nextToken());

			while (st.hasMoreTokens()) {
//			while (scanner.hasNext()) {
				Map<String, Object> rel = new HashMap<String, Object>();
				rel.put("name", st.nextToken());
				rel.put("weight", st.nextToken());
				node.getRelationships().add(rel);
			}

//			scanner.close();
			return node;
		} catch (Exception e) {
			System.err.printf("Could not parse line %d%n%n%s", lineNum+1, e
					.toString());
			return null;
		}
	}

}
