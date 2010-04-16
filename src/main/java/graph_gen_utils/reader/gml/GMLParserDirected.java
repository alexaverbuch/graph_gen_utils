package graph_gen_utils.reader.gml;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.Consts;
import graph_gen_utils.reader.GraphReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;

public class GMLParserDirected implements GraphReader {

	private File graphFile = null;

	public GMLParserDirected(File gmlFile) {
		this.graphFile = gmlFile;
	}

	@Override
	public Iterable<NodeData> getNodes() {
		try {
			return new GmlNodeIterator(graphFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Iterable<NodeData> getRels() {
		try {
			return new GmlRelIterator(graphFile);
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

					if (tokenKey.equals(Consts.GML_ID)) {
						tokenKey = Consts.GID;
						String tokenValStr = st.nextToken();
						tokenVal = Integer.toString(Integer
								.parseInt(tokenValStr));
						hasId = true;
						node.getProperties().put(tokenKey, tokenVal);
						continue;
					}

					if (tokenKey.equals(Consts.WEIGHT)) {
						tokenVal = Double.parseDouble(st.nextToken());
						node.getProperties().put(tokenKey, tokenVal);
						continue;
					}

					if (tokenKey.equals(Consts.COLOR)) {
						tokenVal = Byte.parseByte(st.nextToken());
						node.getProperties().put(tokenKey, tokenVal);
						continue;
					}

					if (tokenKey.equals(Consts.GID))
						continue;

					// NOTE Not fully tested
					// Read entire line, including spaces
					while (st.hasMoreTokens())
						tokenVal = String.format("%s %s", tokenVal, st
								.nextToken());

					node.getProperties().put(tokenKey, tokenVal);

				}

				if (succeeded == false)
					throw new Exception("Unable to parse node!");

				if (node.getProperties().containsKey(Consts.COLOR) == false)
					node.getProperties().put(Consts.COLOR, (byte) -1);

				return node;

			} catch (Exception e) {

				System.err.printf("Could not parse line: %n%s", e.toString());

				return null;

			}
		}

	}

	private class GmlRelIterator implements Iterator<NodeData>,
			Iterable<NodeData> {

		private NodeData nextNodeData = null;
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
			if (nextNodeData != null)
				return true;

			do {
				nextNodeData = parseEntity();

				if (nextNodeData == null)
					break;

				String source = (String) nextNodeData.getProperties().get(
						Consts.GID);
				String target = (String) nextNodeData.getRelationships().get(0)
						.get(Consts.GID);

				if (source.equals(target) == false)
					break;
			} while (true);

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

			do {
				nextNodeData = parseEntity();

				if (nextNodeData == null)
					break;

				String source = (String) nextNodeData.getProperties().get(
						Consts.GID);
				String target = (String) nextNodeData.getRelationships().get(0)
						.get(Consts.GID);

				if (source.equals(target) == false)
					break;
			} while (true);

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

			if (entityStr.equals("]"))
				return null;

			if (entityStr.equals("edge"))
				return parseRel();

			System.err.printf(String.format(
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

					if (tokenKey.equals(Consts.GML_SOURCE)) {
						tokenKey = Consts.GID;
						tokenVal = Integer.toString(Integer.parseInt(st
								.nextToken()));
						hasSource = true;
						node.getProperties().put(tokenKey, tokenVal);
						continue;
					}

					if (tokenKey.equals(Consts.GML_TARGET)) {
						tokenKey = Consts.GID;
						tokenVal = Integer.toString(Integer.parseInt(st
								.nextToken()));
						hasTarget = true;
						rel.put(tokenKey, tokenVal);
						continue;
					}

					if (tokenKey.equals(Consts.GID))
						continue;

					else if (tokenKey.equals(Consts.WEIGHT)) {
						tokenVal = Double.parseDouble(st.nextToken());
						rel.put(tokenKey, tokenVal);
						continue;
					}

					else if (tokenKey.equals(Consts.COLOR)) {
						tokenVal = Byte.parseByte(st.nextToken());
						rel.put(tokenKey, tokenVal);
						continue;
					}

					// NOTE Not fully tested
					// Read entire line, including spaces
					while (st.hasMoreTokens())
						tokenVal = String.format("%s %s", tokenVal, st
								.nextToken());

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

					nextNodeData = parseRel();

					while (true) {
						if (nextNodeData == null)
							break;

						String source = (String) nextNodeData.getProperties()
								.get(Consts.GID);
						String target = (String) nextNodeData
								.getRelationships().get(0).get(Consts.GID);

						if (source.equals(target) == false)
							break;

						nextNodeData = parseEntity();
					}

					return;
				}

				throw new Exception(String.format(
						"Unexpected file format in advanceToRels: %n%s%n",
						tokenStr));

			}
		}

	}

}