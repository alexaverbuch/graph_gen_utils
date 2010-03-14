package graph_gen_utils.gml;

import graph_gen_utils.general.NodeData;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

public class GMLParserUndirected extends GMLParser implements
		Iterator<NodeData> {

	private NodeData nextNodeData = null;
	private Integer nodeNumber = 0;

	public GMLParserUndirected(File gmlFile) throws FileNotFoundException {
		super(gmlFile);
		// skip line, "Creator"
		gmlScanner.nextLine();
		// skip line, "Version"
		gmlScanner.nextLine();
		// skip line, "graph"
		gmlScanner.nextLine();
		// skip line, "["
		gmlScanner.nextLine();
		// skip line, "directed"
		gmlScanner.nextLine();
	}

	@Override
	public boolean hasNext() {
		if (nextNodeData == null) {
			nextNodeData = parseEntity(gmlScanner.nextLine(), nodeNumber);

			if (nextNodeData == null)
				return false;
		}

		return true;
	}

	@Override
	public NodeData next() {
		NodeData entity = null;

		if (nextNodeData != null) {
			entity = nextNodeData;
			nextNodeData = null;
			return entity;
		}

		nextNodeData = parseEntity(gmlScanner.nextLine(), nodeNumber);

		if (nextNodeData != null) {
			entity = nextNodeData;
			nextNodeData = null;
			return entity;
		}

		throw new NoSuchElementException();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	private NodeData parseEntity(String aLine, int nodeNumber) {
		if (gmlScanner.hasNextLine() == false)
			return null;

		StringTokenizer st = new StringTokenizer(gmlScanner.nextLine(), " ");
		String entityStr = st.nextToken();

		if (entityStr.equals("node"))
			return parseNode();

		if (entityStr.equals("edge"))
			return parseRel();

		if (entityStr.equals("]"))
			return null;

		System.out.printf(String.format(
				"Unable to parse entity number: %d%n%n%s", nodeNumber, aLine));

		return null;
	}

	private NodeData parseNode() {
		try {

			boolean succeeded = false;
			boolean hasId = false;

			StringTokenizer st = null;

			st = new StringTokenizer(gmlScanner.nextLine(), " ");
			if (st.nextToken().equals("[") == false)
				throw new Exception("'[' not found!");

			NodeData node = new NodeData();

			while (gmlScanner.hasNextLine()) {

				st = new StringTokenizer(gmlScanner.nextLine(), " ");

				String tokenKey = st.nextToken();
				Object tokenVal = null;

				if (tokenKey.equals("]")) {
					succeeded = hasId;
					break;
				}

				if (tokenKey.equals("id")) {
					tokenKey = "name";
					tokenVal = Integer.parseInt(st.nextToken()) + 1;
					hasId = true;
					node.getProperties().put(tokenKey, tokenVal);
					continue;
				}

				if (tokenKey.equals("weight")) {
					tokenVal = Double.parseDouble(st.nextToken());
					node.getProperties().put(tokenKey, tokenVal);
					continue;
				}

				if (tokenKey.equals("color")) {
					tokenVal = Byte.parseByte(st.nextToken());
					node.getProperties().put(tokenKey, tokenVal);
					continue;
				}

				tokenVal = st.nextToken();
				node.getProperties().put(tokenKey, tokenVal);

			}

			if (succeeded == false)
				throw new Exception("Unable to parse node!");

			return node;

		} catch (Exception e) {

			System.err.printf("Could not parse line %d%n%n%s", nodeNumber, e
					.toString());

			return null;

		}
	}

	private NodeData parseRel() {
		try {

			boolean succeeded = false;
			boolean hasSource = false;
			boolean hasTarget = false;

			StringTokenizer st = null;

			st = new StringTokenizer(gmlScanner.nextLine(), " ");
			if (st.nextToken().equals("[") == false)
				throw new Exception("'[' not found!");

			NodeData node = new NodeData();
			Map<String, Object> rel = new HashMap<String, Object>();

			while (gmlScanner.hasNextLine()) {
				st = new StringTokenizer(gmlScanner.nextLine(), " ");

				String tokenKey = st.nextToken();
				Object tokenVal = null;

				if (tokenKey.equals("]")) {
					succeeded = hasSource && hasTarget;
					break;
				}

				if (tokenKey.equals("source")) {
					tokenKey = "name";
					tokenVal = Integer.parseInt(st.nextToken()) + 1;
					hasSource = true;
					node.getProperties().put(tokenKey, tokenVal);
					continue;
				}

				if (tokenKey.equals("target")) {
					tokenKey = "name";
					tokenVal = Integer.parseInt(st.nextToken()) + 1;
					hasTarget = true;
					rel.put(tokenKey, tokenVal);
					continue;
				}

				else if (tokenKey.equals("weight")) {
					tokenVal = Double.parseDouble(st.nextToken());
					rel.put(tokenKey, tokenVal);
					continue;
				}

				else if (tokenKey.equals("color")) {
					tokenVal = Byte.parseByte(st.nextToken());
					rel.put(tokenKey, tokenVal);
					continue;
				}

				tokenVal = st.nextToken();
				rel.put(tokenKey, tokenVal);

			}

			if (succeeded == false)
				throw new Exception("Unable to parse relationship!");

			node.getRelationships().add(rel);
			return node;

		} catch (Exception e) {

			System.err.printf("Could not parse line %d%n%n%s", nodeNumber + 1,
					e.toString());

			return null;

		}
	}

}