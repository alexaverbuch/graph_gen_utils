package graph_gen_utils.memory_graph;

public class MemRel {

	private Long endNodeId = null;
	private double weight = 1.0;

	public MemRel(Long id, Double weight) {
		super();
		this.endNodeId = id;
		this.weight = weight;
	}

	public Long getEndNodeId() {
		return endNodeId;
	}

	public Double getWeight() {
		return weight;
	}

}
