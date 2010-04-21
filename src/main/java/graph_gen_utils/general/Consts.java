package graph_gen_utils.general;

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
	public static String WEIGHT = "_weight";
	public static String LATITUDE = "_lat";
	public static String LONGITUDE = "_lon";

	// Neo4j Relationship types
	public static String DEFAULT_REL_TYPE_STR = "default";
	public static String INT_REL_TYPE_STR = "internal";
	public static String EXT_REL_TYPE_STR = "external";

	public static final double DEFAULT_REL_WEIGHT = 1.0;

	// Read/Write buffer size
	public static int STORE_BUF = 10000;

	// Default edge weight
	// Must be in range [0-1]
	public static double MIN_EDGE_WEIGHT = 0.05;

	// Change log read timeout (ms)
	public static int CHANGELOG_TIMEOUT = 1000;
	public static int CHANGELOG_MAX_TIMEOUTS = 10;
}
