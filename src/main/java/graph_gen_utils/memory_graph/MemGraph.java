package graph_gen_utils.memory_graph;

import graph_gen_utils.general.Consts;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.uncommons.maths.random.MersenneTwisterRNG;

public class MemGraph implements GraphDatabaseService {

	private Random rng = null;
	private long nextNodeId = -1;

	// Guarantees same order when iterating
	private LinkedHashMap<Long, Node> nodes = null;
	private LinkedHashMap<Long, Relationship> relationships = null;
	private HashMap<Integer, String> propertyKeys = null;

	public MemGraph() {
		super();
		this.rng = new MersenneTwisterRNG();
		this.nodes = new LinkedHashMap<Long, Node>();
		this.relationships = new LinkedHashMap<Long, Relationship>();
		this.propertyKeys = new HashMap<Integer, String>();
	}

	// NOTE Needed because no IdGenerator is used
	// ID must be set before createNode() is called
	public void setNextNodeId(long nextNodeId) {
		this.nextNodeId = nextNodeId;
	}

	// NOTE Needed because no NodeManager is used
	// Called from MemNode. Should NEVER be called from elsewhere
	void removeNode(Long id) {
		if (nodes.remove(id) == null)
			throw new NotFoundException("Node[" + id
					+ "] not found so could not be removed");
	}

	// NOTE Needed because no NodeManager is used
	// Called from MemNode. Should NEVER be called from elsewhere
	void removeRelationship(Long id) {
		if (relationships.remove(id) == null)
			throw new NotFoundException("Relationship[" + id
					+ "] not found so could not be removed");
	}

	// NOTE Needed because no NodeManager is used
	// Called from MemNode. Should NEVER be called from elsewhere
	void addRelationship(MemRel memRel) {
		if (relationships.containsKey(memRel.getId()) == false) {
			relationships.put(memRel.getId(), memRel);
			return;
		}

		throw new NotFoundException("Relationship[" + memRel.getId()
				+ "] not added as it already exists");
	}

	// NOTE Needed to allow MemNode & MemRel to use Integers as property keys
	// Called from MemNode. Should NEVER be called from elsewhere
	void addPropertyKey(String propertyKey) {
		if (propertyKeys.containsKey(propertyKey.hashCode()) == false)
			propertyKeys.put(propertyKey.hashCode(), propertyKey);
	}

	// NOTE Needed to allow MemNode & MemRel to use Integers as property keys
	// Called from MemNode. Should NEVER be called from elsewhere
	String getPropertyKey(Integer propertyKeyHashcode) {
		return propertyKeys.get(propertyKeyHashcode);
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

	@Override
	public KernelEventHandler registerKernelEventHandler(KernelEventHandler arg0) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> TransactionEventHandler<T> registerTransactionEventHandler(
			TransactionEventHandler<T> arg0) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public KernelEventHandler unregisterKernelEventHandler(
			KernelEventHandler arg0) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
			TransactionEventHandler<T> arg0) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

}
