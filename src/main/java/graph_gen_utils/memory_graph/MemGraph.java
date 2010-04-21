package graph_gen_utils.memory_graph;

import graph_gen_utils.general.Consts;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.uncommons.maths.random.MersenneTwisterRNG;

public class MemGraph implements GraphDatabaseService {

	private Random rng = null;
	private long nextNodeId = -1;

	// Guarantees same order when iterating
	private LinkedHashMap<Long, Node> nodes = null;
	private LinkedHashMap<Long, Relationship> relationships = null;

	public MemGraph() {
		super();
		this.rng = new MersenneTwisterRNG();
		this.nodes = new LinkedHashMap<Long, Node>();
		this.relationships = new LinkedHashMap<Long, Relationship>();
	}

	// NOTE Needed because no IdGenerator is used
	// ID must be set before createNode() is called
	public void setNextNodeId(long nextNodeId) {
		this.nextNodeId = nextNodeId;
	}

	// NOTE Needed because no NodeManager is used
	// Called from MemNode. Should NEVER be called from elsewhere
	public void removeNode(Long id) {
		if (nodes.remove(id) == null)
			throw new NotFoundException("Node[" + id
					+ "] not found so could not be removed");
	}

	// NOTE Needed because no NodeManager is used
	// Called from MemNode. Should NEVER be called from elsewhere
	public void removeRelationship(Long id) {
		if (relationships.remove(id) == null)
			throw new NotFoundException("Relationship[" + id
					+ "] not found so could not be removed");
	}

	// NOTE Needed because no NodeManager is used
	// Called from MemNode. Should NEVER be called from elsewhere
	public void addRelationship(MemRel memRel) {
		if (relationships.containsKey(memRel.getId()) == false) {
			relationships.put(memRel.getId(), memRel);
			return;
		}

		throw new NotFoundException("Relationship[" + memRel.getId()
				+ "] not added as it already exists");
	}

	@Override
	public Iterable<Node> getAllNodes() {
		return nodes.values();
	}

	@Override
	public Transaction beginTx() {
		return new MemTransaction();
	}

	@Override
	public Node createNode() {
		if (nextNodeId == -1) {
			// NOTE Exception not thrown due to GraphDatabaseService interface
			throw new Error("NextNodeId has not been set");
		}

		MemNode node = new MemNode(nextNodeId, rng, this);
		node.setProperty(Consts.NODE_GID, node.getId());
		nextNodeId = -1;
		nodes.put(node.getId(), node);
		return node;
	}

	@Override
	public Node getNodeById(long id) {
		Node node = nodes.get(id);
		if (node != null)
			return node;
		throw new NotFoundException("Node[" + id + "] not found");
	}

	@Override
	public Relationship getRelationshipById(long id) {
		Relationship rel = relationships.get(id);
		if (rel != null)
			return rel;
		throw new NotFoundException("Relationship[" + id + "] not found");
	}

	// **********************
	// Unsupported Operations
	// **********************

	@Override
	public boolean enableRemoteShell() {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean enableRemoteShell(Map<String, Serializable> initialProperties) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Node getReferenceNode() {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<RelationshipType> getRelationshipTypes() {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public void shutdown() {
		// NOTE Do nothing silently
		// Provide illusion of real GraphDatabaseService
	}

}
