package graph_gen_utils.general;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class NodeData {

	private Map<String, Object> properties = null;
	private ArrayList<Map<String, Object>> relationships = null;

	public NodeData() {
		this.properties = new HashMap<String, Object>();
		this.relationships = new ArrayList<Map<String, Object>>();
	}

	public Map<String, Object> getProperties() {
		return properties;
	}

	public ArrayList<Map<String, Object>> getRelationships() {
		return relationships;
	}

	// FIXME (Low Priority)
	// Inefficient! Relationships as HashMap would be better
	public boolean containsRelation(Long nodeId) {
		for (Map<String, Object> rel : relationships) {
			if (rel.get(Consts.NODE_GID).equals(nodeId))
				return true;
		}

		return false;
	}

}
