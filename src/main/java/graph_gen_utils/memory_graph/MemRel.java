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
	private HashMap<Integer, Object> properties = null;
	// private HashMap<String, Object> properties = null;
	private long id = -1;

	public MemRel(long id, MemNode startNode, MemNode endNode) {
		super();
		this.startNode = startNode;
		this.endNode = endNode;
		this.properties = new HashMap<Integer, Object>(2);
		// this.properties = new HashMap<String, Object>(4);
		// setProperty(Consts.WEIGHT, Consts.DEFAULT_REL_WEIGHT);
		// this.properties.put(Consts.WEIGHT, Consts.DEFAULT_REL_WEIGHT);
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
		return properties.get(key.hashCode());
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		Vector<String> propertyKeys = new Vector<String>();
		for (Integer propKey : properties.keySet()) {
			propertyKeys.add(startNode.getPropertyKey(propKey));
		}
		return propertyKeys;
		// return properties.keySet();
	}

	@Override
	public Iterable<Object> getPropertyValues() {
		return properties.values();
	}

	@Override
	public boolean hasProperty(String key) {
		return properties.containsKey(key.hashCode());
		// return properties.containsKey(key);
	}

	@Override
	public Object removeProperty(String key) {
		return properties.remove(key.hashCode());
		// return properties.remove(key);
	}

	@Override
	public void setProperty(String key, Object value) {
		properties.put(key.hashCode(), value);
		startNode.addPropertyKey(key);
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
