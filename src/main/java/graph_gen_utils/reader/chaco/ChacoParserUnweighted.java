package graph_gen_utils.reader.chaco;

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

public class ChacoParserUnweighted implements GraphReader {

	private File graphFile = null;

	public ChacoParserUnweighted(File chacoFile) {
		this.graphFile = chacoFile;
	}

	@Override
	public Iterable<NodeData> getNodes() {
		try {
			return new ChacoUnweightedNodeIterator(graphFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Iterable<NodeData> getRels() {
		try {
			return new ChacoUnweightedRelIterator(graphFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private class ChacoUnweightedNodeIterator implements Iterator<NodeData>,
			Iterable<NodeData> {

		private NodeData nextNodeData = null;
		private Scanner chacoScanner = null;
		private Integer nodeNumber = 0;

		public ChacoUnweightedNodeIterator(File chacolFile)
				throws FileNotFoundException {
			this.chacoScanner = new Scanner(chacolFile);

			// skip first line: Nodes Rels ChacoType
			chacoScanner.nextLine();
		}

		@Override
		public Iterator<NodeData> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			if (nextNodeData != null)
				return true;

			nextNodeData = parseNode();

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

			nextNodeData = parseNode();

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

		private NodeData parseNode() {
			try {
				if (chacoScanner.hasNextLine() == false)
					return null;

				nodeNumber++;

				chacoScanner.nextLine();

				NodeData node = new NodeData();

				node.getProperties().put(Consts.NAME,
						Integer.toString(nodeNumber));
				node.getProperties().put(Consts.WEIGHT, 1.0);
				node.getProperties().put(Consts.COLOR, (byte) -1);

				return node;
			} catch (Exception e) {
				e.printStackTrace();
				System.err.printf("Could not parse line %d%n%n%s",
						nodeNumber + 1, e.toString());
				return null;
			}

		}

	}

	private class ChacoUnweightedRelIterator implements Iterator<NodeData>,
			Iterable<NodeData> {

		private NodeData nextNodeData = null;
		private Scanner chacoScanner = null;
		private Integer nodeNumber = 0;

		public ChacoUnweightedRelIterator(File gmlFile) throws Exception {
			this.chacoScanner = new Scanner(gmlFile);

			// skip first line: Nodes Rels ChacoType
			chacoScanner.nextLine();

		}

		@Override
		public Iterator<NodeData> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			if (nextNodeData != null)
				return true;

			nextNodeData = parseNodeAndRels();

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

			nextNodeData = parseNodeAndRels();

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

		private NodeData parseNodeAndRels() {
			try {
				if (chacoScanner.hasNextLine() == false)
					return null;

				nodeNumber++;

				String aLine = chacoScanner.nextLine();

				NodeData node = new NodeData();

				StringTokenizer st = new StringTokenizer(aLine, " ");

				node.getProperties().put(Consts.NAME,
						Integer.toString(nodeNumber));
				node.getProperties().put(Consts.WEIGHT, 1.0);
				node.getProperties().put(Consts.COLOR, (byte) -1);

				// Don't read in edges twice
				while (st.hasMoreTokens()) {
					Map<String, Object> rel = new HashMap<String, Object>();

					Integer toNode = Integer.parseInt(st.nextToken());
					if (toNode <= nodeNumber)
						continue;

					rel.put(Consts.NAME, toNode.toString());
					rel.put(Consts.WEIGHT, 1.0);
					node.getRelationships().add(rel);
				}

				return node;
			} catch (Exception e) {
				e.printStackTrace();
				System.err.printf("Could not parse line %d%n%n%s",
						nodeNumber + 1, e.toString());
				return null;
			}
		}

	}

}