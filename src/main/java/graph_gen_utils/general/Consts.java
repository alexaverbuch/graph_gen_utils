package graph_gen_utils.general;

import org.neo4j.graphdb.RelationshipType;

public abstract class Consts {

	// GML property names
	public static String GML_ID = "id";
	public static String GML_SOURCE = "source";
	public static String GML_TARGET = "target";

	// Neo4j property names
	public static String LID = "_id";
	public static String GID = "_name";
	public static String COLOR = "_color";
	public static String WEIGHT = "_weight";
	public static String LATITUDE = "_lat";
	public static String LONGITUDE = "_lon";

	// Neo4j Relationship types
	public static String DEFAULT_REL_TYPE_STR = "default";

	// General
	public static int STORE_BUF = 10000;

	// Must be in range [0-1]
	public static double MIN_EDGE_WEIGHT = 0.05;

}
