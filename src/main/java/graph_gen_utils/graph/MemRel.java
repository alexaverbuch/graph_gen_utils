package graph_gen_utils.graph;

public class MemRel {

	private Long endNodeId = null;
	private double weight = 1;

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
