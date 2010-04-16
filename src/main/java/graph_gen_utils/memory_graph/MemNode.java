package graph_gen_utils.memory_graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.uncommons.maths.random.ContinuousUniformGenerator;

public class MemNode implements Node {

	private ContinuousUniformGenerator randGen = null;
	private ArrayList<Relationship> relationships = null;
	private HashMap<String, Object> properties = null;
	private MemGraph memGraph = null;
	private long id = -1;

	public MemNode(Long id, Random rng, MemGraph memGraph) {
		this.randGen = new ContinuousUniformGenerator(0.0, 1.0, rng);
		this.relationships = new ArrayList<Relationship>();
		this.properties = new HashMap<String, Object>();
		this.memGraph = memGraph;
		this.id = id;
	}

	public Node getRandomNeighbour(double stayingProbability) throws Exception {
		int neighboursSize = relationships.size();

		if (neighboursSize == 0)
			return this;

		double randVal = randGen.nextValue();

		if (randVal < stayingProbability)
			return this;

		int randIndex = (int) (((randVal - stayingProbability) / (1.0 - stayingProbability)) * neighboursSize);

		if (randIndex >= neighboursSize)
			throw new Exception(String.format(
					"randIndex[%d] >= neighbourSize[%d]\n", randIndex,
					neighboursSize));

		return this.relationships.get(randIndex).getEndNode();
	}

	public Relationship tryGetRelationship(long startNodeId, long endNodeId) {
		for (Relationship rel : relationships)
			if ((rel.getStartNode().getId() == startNodeId)
					&& (rel.getEndNode().getId() == endNodeId))
				return rel;

		return null;
	}

	public void removeRelationship(MemRel memRel) {
		long memRelStartNodeId = memRel.getStartNode().getId();
		long memRelEndNodeId = memRel.getEndNode().getId();

		if (tryGetRelationship(memRelStartNodeId, memRelEndNodeId) != null) {
			relationships.remove(memRelEndNodeId);
			return;
		}

		String errStr = String.format(
				"Relationship[%d->%d] not found in Node[%d]",
				memRelStartNodeId, memRelEndNodeId, getId());

		throw new NotFoundException(errStr);
	}

	public Relationship addRelationship(MemRel memRel) {
		long memRelStartNodeId = memRel.getStartNode().getId();
		long memRelEndNodeId = memRel.getEndNode().getId();

		if (tryGetRelationship(memRelStartNodeId, memRelEndNodeId) == null) {
			relationships.add(memRel);
			return memRel;
		}

		String errStr = String.format(
				"Relationship[%d->%d] already exists in Node[%d]", memRel
						.getStartNode().getId(), memRel.getEndNode().getId(),
				getId());

		throw new NotFoundException(errStr);
	}

	@Override
	public long getId() {
		return id;
	}

	@Override
	public Iterable<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	// TODO implement with iterator later
	public Iterable<Relationship> getRelationships(Direction dir) {

		Iterable<Relationship> result = null;
		ArrayList<Relationship> dirRels = null;

		switch (dir) {
		case BOTH:
			return getRelationships();

		case INCOMING:
			dirRels = new ArrayList<Relationship>();
			for (Relationship rel : relationships)
				if (rel.getEndNode().getId() == getId())
					dirRels.add(rel);
			result = dirRels;
			break;

		case OUTGOING:
			dirRels = new ArrayList<Relationship>();
			for (Relationship rel : relationships)
				if (rel.getEndNode().getId() != getId())
					dirRels.add(rel);
			result = dirRels;
			break;
		}

		return result;
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType type,
			Direction dir) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType... types) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Relationship createRelationshipTo(Node otherNode,
			RelationshipType type) {
		MemRel memRel = new MemRel(this, (MemNode) otherNode);
		((MemNode) otherNode).addRelationship(memRel);
		return addRelationship(memRel);
	}

	@Override
	public void delete() {
		if (relationships.size() > 0) {
			String errStr = String.format(
					"Node[%d] deleted but still has Relationships", getId());
			// NOTE Not possible to throw Exception here due to Node interface
			throw new Error(errStr);
		}
		memGraph.removeNode(getId());
	}

	@Override
	public Relationship getSingleRelationship(RelationshipType type,
			Direction dir) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasRelationship() {
		return relationships.size() > 1;
	}

	@Override
	public boolean hasRelationship(RelationshipType... types) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasRelationship(Direction dir) {
		switch (dir) {
		case BOTH:
			return hasRelationship();

		case INCOMING:
			for (Relationship rel : relationships)
				if (rel.getEndNode().getId() == getId())
					return true;
			break;

		case OUTGOING:
			for (Relationship rel : relationships)
				if (rel.getEndNode().getId() != getId())
					return true;
			break;
		}

		return false;
	}

	@Override
	public boolean hasRelationship(RelationshipType type, Direction dir) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Traverser traverse(Order traversalOrder,
			StopEvaluator stopEvaluator,
			ReturnableEvaluator returnableEvaluator,
			Object... relationshipTypesAndDirections) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Traverser traverse(Order traversalOrder,
			StopEvaluator stopEvaluator,
			ReturnableEvaluator returnableEvaluator,
			RelationshipType relationshipType, Direction direction) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Traverser traverse(Order traversalOrder,
			StopEvaluator stopEvaluator,
			ReturnableEvaluator returnableEvaluator,
			RelationshipType firstRelationshipType, Direction firstDirection,
			RelationshipType secondRelationshipType, Direction secondDirection) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getProperty(String key) {
		return properties.get(key);
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		return properties.keySet();
	}

	@Override
	public Iterable<Object> getPropertyValues() {
		return properties.values();
	}

	@Override
	public boolean hasProperty(String key) {
		return properties.containsKey(key);
	}

	@Override
	public Object removeProperty(String key) {
		return properties.remove(key);
	}

	@Override
	public void setProperty(String key, Object value) {
		properties.put(key, value);
	}

}
