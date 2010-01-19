package graph_gen_utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class NodeData {
	
	private Map<String, Object> properties;
	private ArrayList<Map<String, Object>> relationships;
	
	public NodeData() {
		this.properties = new HashMap<String,Object>();
		this.relationships = new ArrayList<Map<String,Object>>();
	}

	public Map<String, Object> getProperties() {
		return properties;
	}

	public ArrayList<Map<String, Object>> getRelationships() {
		return relationships;
	}

	
}
