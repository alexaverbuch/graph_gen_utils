package graph_gen_utils.memory_graph;

import java.util.HashMap;
import java.util.Vector;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class MemRel implements Relationship {

	private MemNode startNode = null;
	private MemNode endNode = null;

	// TODO UNCOMMENT (performance)
	// private HashMap<String, Object> properties = null;

	// TODO UNCOMMENT (performance)
	// private long id = -1;
	private int id = -1;

	public MemRel(int id, MemNode startNode, MemNode endNode) {
		super();
		this.startNode = startNode;
		this.endNode = endNode;

		// TODO UNCOMMENT (performance)
		// this.properties = new HashMap<String, Object>(2);
		// setProperty(Consts.WEIGHT, Consts.DEFAULT_REL_WEIGHT);

		// TODO UNCOMMENT (performance)
		// this.id = id;
		this.id = id;
	}

	@Override
	public long getId() {
		return id;
	}

	@Override
	public void delete() {
		startNode.removeRelationship(this);
		endNode.removeRelationship(this);
	}

	@Override
	public Node getStartNode() {
		return startNode;
	}

	@Override
	public Node getEndNode() {
		return endNode;
	}

	@Override
	public Node getOtherNode(Node node) {
		if (node.getId() == endNode.getId())
			return startNode;
		else if (node.getId() == startNode.getId())
			return endNode;

		throw new NotFoundException("Node[" + node.getId()
				+ "] not connected to this Relationship[" + getId() + "]");
	}

	@Override
	public Node[] getNodes() {
		return new Node[] { startNode, endNode };
	}

	@Override
	public Object getProperty(String key) {
		// TODO UNCOMMENT (performance)
		// return properties.get(key);
		return 1d;
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		// TODO UNCOMMENT (performance)
		// return properties.keySet();
		return null;
	}

	@Override
	public Iterable<Object> getPropertyValues() {
		// TODO UNCOMMENT (performance)
		// return properties.values();
		return null;
	}

	@Override
	public boolean hasProperty(String key) {
		// TODO UNCOMMENT (performance)
		// return properties.containsKey(key);
		return false;
	}

	@Override
	public Object removeProperty(String key) {
		// TODO UNCOMMENT (performance)
		// return properties.remove(key);
		return null;
	}

	@Override
	public void setProperty(String key, Object value) {
		// TODO UNCOMMENT (performance)
		// properties.put(key, value);
	}

	// **********************
	// Unsupported Operations
	// **********************

	@Override
	public RelationshipType getType() {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isType(RelationshipType type) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public GraphDatabaseService getGraphDatabase() {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

}
