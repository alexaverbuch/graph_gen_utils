package graph_gen_utils.memory_graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.uncommons.maths.random.MersenneTwisterRNG;

public class MemGraph {

	private Random rng = null;

	private Map<Long, MemNode> nodes = null;

	public MemGraph() {
		super();
		this.rng = new MersenneTwisterRNG();
		this.nodes = new HashMap<Long, MemNode>();
	}

	public void addNode(Long id, Byte color) {
		MemNode node = new MemNode(id, color, rng);
		this.nodes.put(id, node);
	}

	public void addNode(Long id, MemNode node) {
		this.nodes.put(id, node);
	}

	public void removeNode(Long id) {
		this.nodes.remove(id);
	}

	public MemNode getNode(Long id) {
		return this.nodes.get(id);
	}

	public Collection<MemNode> getAllNodes() {
		return this.nodes.values();
	}

	// May be useful for defining order returned by getAllNodes()
	// NOTE Order returned by MemNode.getNeighbours() still undefined
	// NOTE Degrades insertion performance & expensive on larger graphs
	public void sortNodesByKey() {
		this.nodes = new TreeMap<Long, MemNode>(this.nodes);
	}

}
