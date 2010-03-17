package graph_gen_utils.gml;

import graph_gen_utils.general.NodeData;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Map.Entry;

public class GMLParserUndirected extends GMLParser {

	public GMLParserUndirected(File gmlFile) throws FileNotFoundException {
		super(gmlFile);
	}

	@Override
	public Iterable<NodeData> getNodes() {
		try {
			return new GmlNodeIterator(gmlFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Iterable<NodeData> getRels() {
		try {
			return new GmlRelIterator(gmlFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private class GmlNodeIterator implements Iterator<NodeData>,
			Iterable<NodeData> {

		private NodeData nextNodeData = null;
		private Scanner gmlScanner = null;

		public GmlNodeIterator(File gmlFile) throws FileNotFoundException {
			this.gmlScanner = new Scanner(gmlFile);

			// skip line, "Creator"
			gmlScanner.nextLine();

			// skip line, "Version"
			gmlScanner.nextLine();

			// skip line, "graph"
			gmlScanner.nextLine();

			// skip line, "["
			gmlScanner.nextLine();

			// skip line, "directed"s
			gmlScanner.nextLine();
		}

		@Override
		public Iterator<NodeData> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			if (nextNodeData != null)
				return true;

			nextNodeData = parseEntity();

			return (nextNodeData != null);
		}

		@Override
		public NodeData next() {
			NodeData nodeData = null;

			if (nextNodeData != null) {
				nodeData = nextNodeData;
				nextNodeData = null;
				return nodeData;
			}

			nextNodeData = parseEntity();

			if (nextNodeData != null) {
				nodeData = nextNodeData;
				nextNodeData = null;
				return nodeData;
			}

			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		private NodeData parseEntity() {
			if (gmlScanner.hasNextLine() == false)
				return null;

			StringTokenizer st = new StringTokenizer(gmlScanner.nextLine());
			String entityStr = st.nextToken();

			if (entityStr.equals("edge"))
				return null;

			if (entityStr.equals("]"))
				return null;

			if (entityStr.equals("node"))
				return parseNode();

			System.err
					.printf(String
							.format(
									"GmlNodeIterator.parseEntity(): Unable to parse line: %n%s",
									entityStr));

			return null;
		}

		private NodeData parseNode() {
			try {

				boolean succeeded = false;
				boolean hasId = false;

				StringTokenizer st = null;

				st = new StringTokenizer(gmlScanner.nextLine());
				if (st.nextToken().equals("[") == false)
					throw new Exception("'[' not found!");

				NodeData node = new NodeData();

				while (gmlScanner.hasNextLine()) {

					st = new StringTokenizer(gmlScanner.nextLine());

					String tokenKey = st.nextToken();
					Object tokenVal = null;

					if (tokenKey.equals("]")) {
						succeeded = hasId;
						break;
					}

					if (tokenKey.equals("id")) {
						tokenKey = "name";
						String tokenValStr = st.nextToken();
						tokenVal = Integer.toString(Integer
								.parseInt(tokenValStr) + 1);
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

					if (tokenKey.equals("name"))
						continue;

					tokenVal = st.nextToken();
					node.getProperties().put(tokenKey, tokenVal);

				}

				if (succeeded == false)
					throw new Exception("Unable to parse node!");

				if (node.getProperties().containsKey("color") == false)
					node.getProperties().put("color", (byte) -1);

				return node;

			} catch (Exception e) {

				System.err.printf("Could not parse line: %n%s", e.toString());

				return null;

			}
		}

	}

	private class GmlRelIterator implements Iterator<NodeData>,
			Iterable<NodeData> {

		private NodeData nextNodeDataTo = null;
		private NodeData nextNodeDataFrom = null;
		private Scanner gmlScanner = null;

		public GmlRelIterator(File gmlFile) throws Exception {
			this.gmlScanner = new Scanner(gmlFile);

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

			// skip lines, all nodes
			advanceToRels();
		}

		@Override
		public Iterator<NodeData> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			if (nextNodeDataTo != null)
				return true;

			if (nextNodeDataFrom != null)
				return true;

			do {
				nextNodeDataTo = parseEntity();

				if (nextNodeDataTo == null)
					break;

				String source = (String) nextNodeDataTo.getProperties().get(
						"name");
				String target = (String) nextNodeDataTo.getRelationships().get(
						0).get("name");

				if (source.equals(target) == false)
					break;

				// System.out.println("\n Source = Target!");
			} while (true);

			if (nextNodeDataTo != null)
				nextNodeDataFrom = reverseRel(nextNodeDataTo);

			return (nextNodeDataTo != null);
		}

		@Override
		public NodeData next() {
			NodeData nodeData = null;

			if (nextNodeDataTo != null) {
				nodeData = nextNodeDataTo;
				nextNodeDataTo = null;
				return nodeData;
			}

			if (nextNodeDataFrom != null) {
				nodeData = nextNodeDataFrom;
				nextNodeDataFrom = null;
				return nodeData;
			}

			do {
				nextNodeDataTo = parseEntity();

				if (nextNodeDataTo == null)
					break;

				String source = (String) nextNodeDataTo.getProperties().get(
						"name");
				String target = (String) nextNodeDataTo.getRelationships().get(
						0).get("name");

				if (source.equals(target) == false)
					break;

				// System.out.println("\n Source = Target!");
			} while (true);

			if (nextNodeDataTo != null) {
				nextNodeDataFrom = reverseRel(nextNodeDataTo);
				nodeData = nextNodeDataTo;
				nextNodeDataTo = null;
				return nodeData;
			}

			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		private NodeData parseEntity() {
			if (gmlScanner.hasNextLine() == false)
				return null;

			StringTokenizer st = new StringTokenizer(gmlScanner.nextLine());
			String entityStr = st.nextToken();

			if (entityStr.equals("]"))
				return null;

			if (entityStr.equals("edge"))
				return parseRel();

			// FIXME remove
			System.out.printf(String.format(
					// System.err.printf(String.format(
					"GmlRelIterator.parseEntity(): Unable to parse line: %n%s",
					entityStr));

			return null;
		}

		private NodeData parseRel() {
			try {

				boolean succeeded = false;
				boolean hasSource = false;
				boolean hasTarget = false;

				StringTokenizer st = null;

				st = new StringTokenizer(gmlScanner.nextLine());

				if (st.nextToken().equals("[") == false)
					throw new Exception("'[' not found!");

				NodeData node = new NodeData();
				Map<String, Object> rel = new HashMap<String, Object>();

				while (gmlScanner.hasNextLine()) {
					st = new StringTokenizer(gmlScanner.nextLine());

					String tokenKey = st.nextToken();
					Object tokenVal = null;

					if (tokenKey.equals("]")) {
						succeeded = hasSource && hasTarget;
						break;
					}

					if (tokenKey.equals("source")) {
						tokenKey = "name";
						tokenVal = Integer.toString(Integer.parseInt(st
								.nextToken()) + 1);
						hasSource = true;
						node.getProperties().put(tokenKey, tokenVal);
						continue;
					}

					if (tokenKey.equals("target")) {
						tokenKey = "name";
						tokenVal = Integer.toString(Integer.parseInt(st
								.nextToken()) + 1);
						hasTarget = true;
						rel.put(tokenKey, tokenVal);
						continue;
					}

					if (tokenKey.equals("name"))
						continue;

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
					throw new Exception("Unable to parse rel!");

				node.getRelationships().add(rel);
				return node;

			} catch (Exception e) {

				System.err.printf("Could not parse line: %n%s", e.toString());

				return null;

			}
		}

		private void advanceToRels() throws Exception {
			StringTokenizer st = null;

			while (gmlScanner.hasNextLine()) {
				st = new StringTokenizer(gmlScanner.nextLine());

				String tokenStr = st.nextToken();

				if (tokenStr.equals("]"))
					return;

				if (tokenStr.equals("node")) {

					while (gmlScanner.hasNextLine()) {
						st = new StringTokenizer(gmlScanner.nextLine());
						if (st.nextToken().equals("]"))
							break;
					}

					continue;

				}

				if (tokenStr.equals("edge")) {

					nextNodeDataTo = parseRel();

					while (true) {
						if (nextNodeDataTo == null)
							break;

						String source = (String) nextNodeDataTo.getProperties()
								.get("name");
						String target = (String) nextNodeDataTo
								.getRelationships().get(0).get("name");

						if (source.equals(target) == false)
							break;

						nextNodeDataTo = parseEntity();
					}

					if (nextNodeDataTo != null)
						nextNodeDataFrom = reverseRel(nextNodeDataTo);

					// nextNodeDataTo = parseRel();
					//					
					// if (nextNodeDataTo != null)
					// nextNodeDataFrom = reverseRel(nextNodeDataTo);
					return;
				}

				String errMsg = String.format(
						"Unexpected file format in advanceToRels: %n%s%n",
						tokenStr);
				throw new Exception(errMsg);

			}
		}

		private NodeData reverseRel(NodeData from) {
			NodeData to = new NodeData();
			String toTarget = null;
			String toSource = null;

			for (Entry<String, Object> fromProp : from.getProperties()
					.entrySet()) {

				if (fromProp.getKey().equals("name")) {
					toTarget = (String) fromProp.getValue();
					continue;
				}

				to.getProperties().put(fromProp.getKey(), fromProp.getValue());

			}

			for (Map<String, Object> fromRel : from.getRelationships()) {

				Map<String, Object> toRel = new HashMap<String, Object>();

				for (Entry<String, Object> fromRelProp : fromRel.entrySet()) {

					if (fromRelProp.getKey().equals("name")) {
						toSource = (String) fromRelProp.getValue();
						toRel.put(fromRelProp.getKey(), toTarget);
						continue;
					}

					toRel.put(fromRelProp.getKey(), fromRelProp.getValue());

				}

				to.getRelationships().add(toRel);
			}

			to.getProperties().put("name", toSource);

			return to;
		}

	}

}