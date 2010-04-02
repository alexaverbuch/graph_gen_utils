package graph_gen_utils.memory_graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.Random;

import org.uncommons.maths.random.ContinuousUniformGenerator;

public class MemNode {

	private ContinuousUniformGenerator randGen = null;

	private HashMap<Long, MemRel> neighbours = null;
	private byte color = -1;
	private long id = -1;

	public MemNode(Long id, byte color, Random rng) {
		this.neighbours = new HashMap<Long, MemRel>();
		this.color = color;
		this.id = id;
		this.randGen = new ContinuousUniformGenerator(0.0, 1.0, rng);
	}

	public long getRandomNeighbour(double stayingProbability) throws Exception {
		int neighboursSize = neighbours.size();

		if (neighboursSize == 0)
			return this.id;

		double randVal = randGen.nextValue();

		if (randVal < stayingProbability)
			return this.id;

		int randIndex = (int) (((randVal - stayingProbability) / (1.0 - stayingProbability)) * neighboursSize);

		if (randIndex >= neighboursSize)
			throw new Exception(String.format(
					"randIndex[%d] >= neighbourSize[%d]\n", randIndex,
					neighboursSize));

		return this.neighbours.get(randIndex).getEndNodeId();
	}

	public int getNeighbourCount() {
		return neighbours.size();
	}

	public void addNeighbour(MemRel rel) {
		neighbours.put(rel.getEndNodeId(), rel);
	}

	public Collection<MemRel> getNeighbours() {
		return neighbours.values();
	}

	public boolean hasNeighbour(Long id) {
		return this.neighbours.containsKey(id);
	}

	public Byte getColor() {
		return color;
	}

	public void setColor(Byte color) {
		this.color = color;
	}

	public Long getId() {
		return id;
	}

}
