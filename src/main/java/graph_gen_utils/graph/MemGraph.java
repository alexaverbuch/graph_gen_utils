package graph_gen_utils.graph;

import java.util.HashMap;
import java.util.Random;

import org.uncommons.maths.random.MersenneTwisterRNG;

public class MemGraph {

	private Random rng = null;

	private HashMap<Long, MemNode> nodes = new HashMap<Long, MemNode>();

	public MemGraph() {
		super();
		this.rng = new MersenneTwisterRNG();
		this.nodes = new HashMap<Long, MemNode>();
	}

	public void addNode(Long id, Integer color) {
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
}
