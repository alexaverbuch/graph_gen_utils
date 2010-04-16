package graph_gen_utils.memory_graph;

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
	private long nextId = -1;

	// Guarantees same order when iterating
	private LinkedHashMap<Long, Node> nodes = null;

	public MemGraph() {
		super();
		this.rng = new MersenneTwisterRNG();
		this.nodes = new LinkedHashMap<Long, Node>();
	}

	// NOTE Needed because no IdGenerator is used
	// ID must be set before createNode() is called
	public void setNextId(long nextId) {
		this.nextId = nextId;
	}

	// NOTE Needed because no NodeManager is used
	// Called from MemNode. Should NEVER be called from elsewhere
	public void removeNode(Long id) {
		if (nodes.remove(id) == null)
			throw new NotFoundException("Node[" + id
					+ "] not found so could not be removed");
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
		if (nextId == -1) {
			// NOTE Exception not thrown due to GraphDatabaseService interface
			throw new Error("NextId has not been set");
		}

		MemNode node = new MemNode(nextId, rng, this);
		nextId = -1;
		nodes.put(node.getId(), node);
		return node;
	}

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
	public Node getNodeById(long id) {
		Node node = nodes.get(id);
		if (node != null)
			return node;
		throw new NotFoundException("Node[" + id + "] not found");
	}

	@Override
	public Node getReferenceNode() {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Relationship getRelationshipById(long id) {
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
