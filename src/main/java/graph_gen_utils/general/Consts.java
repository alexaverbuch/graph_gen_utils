package graph_gen_utils.general;

import org.neo4j.graphdb.RelationshipType;

public abstract class Consts {

	// GML property names
	public static String GML_ID = "id";
	public static String GML_SOURCE = "source";
	public static String GML_TARGET = "target";

	// Neo4j property names
	public static String NODE_LID = "_n_lid";
	public static String NODE_GID = "_n_gid";
	public static String REL_GID = "_r_gid";
	public static String COLOR = "_color";
	public static String WEIGHT = "weight";
	public static String LATITUDE = "lat";
	public static String LONGITUDE = "lon";
	public static String NAME = "name";

	// Neo4j Relationship types
	public static enum RelationshipTypes implements RelationshipType {
		DEFAULT, INTERNAL, EXTERNAL
	}

	public static final double DEFAULT_REL_WEIGHT = 1.0;

	// Read/Write buffer size
	public static int STORE_BUF = 10000;

	// Default edge weight
	// Must be in range [0-1]
	public static double MIN_EDGE_WEIGHT = 0.05;

	// Change log read timeout (ms)
	// public static int CHANGELOG_TIMEOUT = 1000;
	// public static int CHANGELOG_MAX_TIMEOUTS = 10;
	public static int CHANGELOG_TIMEOUT = 1;
	public static int CHANGELOG_MAX_TIMEOUTS = 1;

	// ChangeOp log operation names
	public static String CHANGELOG_OP_ADD_NODE = "Add_Node";
	public static String CHANGELOG_OP_ADD_RELATIONSIHP = "Add_Rel";
	public static String CHANGELOG_OP_DELETE_NODE = "Del_Node";
	public static String CHANGELOG_OP_DELETE_RELATIONSHIP = "Del_Rel";
}
