package graph_gen_utils.general;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class NodeData {

	private Map<String, Object> properties = null;
	private ArrayList<Map<String, Object>> relationships = null;
	private HashMap<String, Map<String, Object>> NEWrelationships = null;

	public NodeData() {
		this.properties = new HashMap<String, Object>();
		this.relationships = new ArrayList<Map<String, Object>>();
		this.NEWrelationships = new HashMap<String, Map<String, Object>>();
	}

	public Map<String, Object> getProperties() {
		return properties;
	}

	public ArrayList<Map<String, Object>> getRelationships() {
		return relationships;
	}

	public HashMap<String, Map<String, Object>> NEWgetRelationships() {
		return NEWrelationships;
	}

	public ArrayList<Map<String, Object>> NEWgetRelationshipsCollection() {		
		return (ArrayList<Map<String, Object>>) NEWrelationships.values();
	}

}
