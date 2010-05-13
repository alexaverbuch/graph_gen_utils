package graph_gen_utils.reader.twitter;

import graph_gen_utils.general.NodeData;
import graph_gen_utils.general.Consts;
import graph_gen_utils.reader.GraphReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;

public class TwitterParser implements GraphReader {

	private File graphFile = null;

	public TwitterParser(File twitterFile) {
		this.graphFile = twitterFile;
	}

	@Override
	public Iterable<NodeData> getNodes() {
		try {
			return new TwitterNodeIterator(graphFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Iterable<NodeData> getRels() {
		try {
			return new TwitterRelIterator(graphFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private class TwitterNodeIterator implements Iterator<NodeData>,
			Iterable<NodeData> {

		private final int NODE_SIZE = 4;
		private final int NODES_PER_READ = 10000;

		private NodeData nextNodeData = null;
		private ReadableByteChannel channel = null;
		private ByteBuffer byteBuf = null;
		private IntBuffer intBuf = null;
		private HashSet<Integer> nodes = new HashSet<Integer>();
		private int nodesInBuf = 0;

		public TwitterNodeIterator(File graphFile) throws FileNotFoundException {
			this.channel = new FileInputStream(graphFile).getChannel();
			this.byteBuf = ByteBuffer.allocate(NODE_SIZE * NODES_PER_READ);

			// As defined by the dataset file format
			this.byteBuf.order(ByteOrder.LITTLE_ENDIAN);

			this.byteBuf.rewind();
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

				Integer nodeId = 0;
				do {
					if (nodesInBuf <= 0) {
						// NOTE No more Nodes remain in buffer

						// read() places bytes at tbuffer's position
						// position must be set before calling read()
						// set position to 0
						byteBuf.rewind();

						if ((nodesInBuf = channel.read(byteBuf) / NODE_SIZE) <= 0)
							// NOTE No more Nodes remain in file
							return null;

						// read() also moves position
						// to read bytes buffer's position must be reset to 0
						byteBuf.rewind();

						intBuf = byteBuf.asIntBuffer();
					}

					nodeId = intBuf.get();
					nodesInBuf--;

					// NOTE: Don't return duplicate Node IDs
				} while (nodes.add(nodeId) == false);

				NodeData node = new NodeData();

				node.getProperties().put(Consts.NODE_GID, new Long(nodeId));

				return node;

			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
	}

	private class TwitterRelIterator implements Iterator<NodeData>,
			Iterable<NodeData> {

		private final int REL_SIZE = 8;
		private final int RELS_PER_READ = 10000;

		private NodeData nextNodeData = null;
		private ReadableByteChannel channel = null;
		private ByteBuffer byteBuf = null;
		private IntBuffer intBuf = null;
		private int relsInBuf = 0;

		public TwitterRelIterator(File gmlFile) throws Exception {
			this.channel = new FileInputStream(graphFile).getChannel();
			this.byteBuf = ByteBuffer.allocate(REL_SIZE * RELS_PER_READ);

			// As defined by the dataset file format
			this.byteBuf.order(ByteOrder.LITTLE_ENDIAN);

			this.byteBuf.rewind();
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

				if (relsInBuf <= 0) {
					// NOTE No more Relationships remain in buffer

					// read() places bytes at tbuffer's position
					// position must be set before calling read()
					// set position to 0
					byteBuf.rewind();

					if ((relsInBuf = channel.read(byteBuf) / REL_SIZE) <= 0)
						// NOTE No more Relationships remain in file
						return null;

					// read() also moves position
					// to read bytes buffer's position must be reset to 0
					byteBuf.rewind();

					intBuf = byteBuf.asIntBuffer();
				}

				NodeData nodeAndRel = new NodeData();

				Integer sourceId = intBuf.get();
				nodeAndRel.getProperties().put(Consts.NODE_GID,
						new Long(sourceId));

				Integer destId = intBuf.get();
				Map<String, Object> rel = new HashMap<String, Object>();
				rel.put(Consts.NODE_GID, new Long(destId));

				nodeAndRel.getRelationships().add(rel);

				relsInBuf--;

				return nodeAndRel;

			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

	}

}