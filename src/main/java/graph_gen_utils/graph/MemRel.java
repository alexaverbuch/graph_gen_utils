package graph_gen_utils.graph;

public class MemRel {
	
	private Long endNodeId = null;
	private Double weight = null;
	
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
