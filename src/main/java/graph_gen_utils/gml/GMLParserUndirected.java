package graph_gen_utils.gml;

import graph_gen_utils.general.NodeData;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;

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
		}
	}

	private class GmlNodeIterator implements Iterator<NodeData>, Iterable<NodeData> {

		private NodeData nextNodeData = null;
		private Integer entityNumber = 0;
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
			// skip line, "directed"
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

			nextNodeData = parseEntity(gmlScanner.nextLine());

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

			nextNodeData = parseEntity(gmlScanner.nextLine());

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

		private NodeData parseEntity(String aLine) {
			if (gmlScanner.hasNextLine() == false)
				return null;

			StringTokenizer st = new StringTokenizer(gmlScanner.nextLine(), " ");
			String entityStr = st.nextToken();

			if (entityStr.equals("edge"))
				return null;

			if (entityStr.equals("]"))
				return null;

			if (entityStr.equals("node"))
				return parseNode();

			System.out.printf(String.format(
					"Unable to parse entity number: %d%n%n%s", entityNumber,
					aLine));

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

				System.err.printf("Could not parse line %d%n%n%s",
						entityNumber, e.toString());

				return null;

			}
		}

	}

	private class GmlRelIterator implements Iterator<NodeData>, Iterable<NodeData> {

		private NodeData nextNodeData = null;
		private Integer entityNumber = 0;
		private Scanner gmlScanner = null;

		public GmlRelIterator(File gmlFile) throws FileNotFoundException {
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
			// skip line, "directed"
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

			nextNodeData = parseEntity(gmlScanner.nextLine());

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

			nextNodeData = parseEntity(gmlScanner.nextLine());

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

		private NodeData parseEntity(String aLine) {
			if (gmlScanner.hasNextLine() == false)
				return null;

			StringTokenizer st = new StringTokenizer(gmlScanner.nextLine(), " ");
			String entityStr = st.nextToken();

			if (entityStr.equals("node"))
				return null;

			if (entityStr.equals("]"))
				return null;

			if (entityStr.equals("edge"))
				return parseRel();

			System.out.printf(String.format(
					"Unable to parse entity number: %d%n%n%s", entityNumber,
					aLine));

			return null;
		}

		private NodeData parseRel() {
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

				System.err.printf("Could not parse line %d%n%n%s",
						entityNumber, e.toString());

				return null;

			}
		}

		private void advanceToRels() {
			StringTokenizer st = null;

			st = new StringTokenizer(gmlScanner.nextLine(), " ");
			if (st.nextToken().equals("]"))
				return;

			while (gmlScanner.hasNextLine()) {
				st = new StringTokenizer(gmlScanner.nextLine(), " ");

				String tokenStr = st.nextToken();

				if (tokenStr.equals("node")) {

					while (gmlScanner.hasNextLine()) {
						st = new StringTokenizer(gmlScanner.nextLine(), " ");
						if (st.nextToken().equals("]"))
							break;
					}

					continue;

				}

				if (tokenStr.equals("edge"))
					nextNodeData = parseRel();

			}
		}

	}

}