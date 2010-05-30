package graph_gen_utils.memory_graph;

import graph_gen_utils.general.Consts;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Vector;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
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

	// private ContinuousUniformGenerator randGen = null;
	private LinkedHashMap<Long, Relationship> relationships = null;

	// TODO UNCOMMENT (performance)
	// private HashMap<String, Object> properties = null;
	private Byte color = null;

	private MemGraph memGraph = null;
	private long id = -1;
	private long nextRelId = -1;

	public MemNode(Long id, Random rng, MemGraph memGraph) {
		// this.randGen = new ContinuousUniformGenerator(0.0, 1.0, rng);
		this.relationships = new LinkedHashMap<Long, Relationship>(8);

		// TODO UNCOMMENT (performance)
		// this.properties = new HashMap<String, Object>(4);
		color = -1;

		this.memGraph = memGraph;
		this.id = id;
	}

	// TODO Uncomment later?
	// public Node getRandomNeighbour(double stayingProbability) throws
	// Exception {
	// int neighboursSize = relationships.size();
	//
	// if (neighboursSize == 0)
	// return this;
	//
	// double randVal = randGen.nextValue();
	//
	// if (randVal < stayingProbability)
	// return this;
	//
	// int randIndex = (int) (((randVal - stayingProbability) / (1.0 -
	// stayingProbability)) * neighboursSize);
	//
	// if (randIndex >= neighboursSize)
	// throw new Exception(String.format(
	// "randIndex[%d] >= neighbourSize[%d]\n", randIndex,
	// neighboursSize));
	//
	// int currIndex = 0;
	// for (Relationship rel : relationships.values()) {
	// if (currIndex == randIndex)
	// return rel.getEndNode();
	// currIndex++;
	// }
	//
	// throw new Exception("Unable to retrieve random node");
	// }

	void removeRelationship(MemRel memRel) {
		if (relationships.containsKey(memRel.getId()) == true) {
			relationships.remove(memRel.getId());
			if (memRel.getStartNode().getId() == getId())
				memGraph.removeRelationship(memRel.getId());
			return;
		}

		String errStr = String.format("Relationship[%d] not found in Node[%d]",
				memRel.getId(), getId());

		throw new NotFoundException(errStr);
	}

	// NOTE Needed because no IdGenerator is used
	// ID must be set before createRelationship() is called
	public void setNextRelId(long nextRelId) {
		this.nextRelId = nextRelId;
	}

	public Relationship addRelationship(MemRel memRel) {
		if (relationships.containsKey(memRel.getId()) == false) {
			relationships.put(memRel.getId(), memRel);
			if (memRel.getStartNode().getId() == getId())
				memGraph.addRelationship(memRel);
			return memRel;
		}

		String errStr = String.format(
				"Relationship[%d] already exists in Node[%d]", memRel.getId(),
				getId());

		throw new NotFoundException(errStr);
	}

	@Override
	public long getId() {
		return id;
	}

	@Override
	public Iterable<Relationship> getRelationships() {
		return relationships.values();
	}

	@Override
	public Iterable<Relationship> getRelationships(Direction dir) {
		if (dir == Direction.BOTH)
			return relationships.values();
		return new RelationshipIterator(dir);
	}

	@Override
	public Relationship createRelationshipTo(Node otherNode,
			RelationshipType type) {
		if (nextRelId == -1) {
			// NOTE Exception not thrown due to GraphDatabaseService interface
			throw new Error("NextRelId has not been set");
		}

		MemRel memRel = new MemRel(nextRelId, this, (MemNode) otherNode);
		memRel.setProperty(Consts.REL_GID, memRel.getId());
		nextRelId = -1;
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
	public boolean hasRelationship() {
		return relationships.size() > 1;
	}

	@Override
	public boolean hasRelationship(Direction dir) {
		switch (dir) {
		case BOTH:
			return hasRelationship();

		case INCOMING:
			for (Relationship rel : relationships.values())
				if (rel.getEndNode().getId() == getId())
					return true;
			break;

		case OUTGOING:
			for (Relationship rel : relationships.values())
				if (rel.getEndNode().getId() != getId())
					return true;
			break;
		}

		return false;
	}

	@Override
	// NOTE This is slow, but used to reduce memory footprint
	public Iterable<String> getPropertyKeys() {
		// TODO UNCOMMENT (performance)
		// return properties.keySet();
		return null;
	}

	@Override
	public Object getProperty(String key) {
		// TODO UNCOMMENT (performance)
		// return properties.get(key);
		return color;
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
		if (key.equals(Consts.COLOR))
			color = (Byte) value;
	}

	// **********************
	// Unsupported Operations
	// **********************

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
	public Relationship getSingleRelationship(RelationshipType type,
			Direction dir) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasRelationship(RelationshipType... types) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
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
	public Object getProperty(String key, Object defaultValue) {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

	private class RelationshipIterator implements Iterator<Relationship>,
			Iterable<Relationship> {

		private Direction direction = null;
		private Iterator<Relationship> relationshipIter = null;
		private Relationship nextRelationship = null;
		private Relationship tempNextRelationship = null;

		public RelationshipIterator(Direction direction) {
			this.direction = direction;
			this.relationshipIter = relationships.values().iterator();
			this.nextRelationship = null;
			this.tempNextRelationship = null;
		}

		@Override
		public boolean hasNext() {
			while (relationshipIter.hasNext()) {
				if (nextRelationship == null)
					nextRelationship = relationshipIter.next();

				if ((direction == Direction.OUTGOING)
						&& (nextRelationship.getStartNode().getId() == getId()))
					return true;

				if ((direction == Direction.INCOMING)
						&& (nextRelationship.getEndNode().getId() == getId()))
					return true;

				if (direction == Direction.BOTH)
					return true;

				nextRelationship = null;
			}

			return false;
		}

		@Override
		public Relationship next() {
			if (nextRelationship != null) {
				tempNextRelationship = nextRelationship;
				nextRelationship = null;
				return tempNextRelationship;
			}

			if (hasNext()) {
				tempNextRelationship = nextRelationship;
				nextRelationship = null;
				return tempNextRelationship;
			}

			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<Relationship> iterator() {
			return this;
		}

	}

	@Override
	public GraphDatabaseService getGraphDatabase() {
		// NOTE Not Supported
		throw new UnsupportedOperationException();
	}

}
