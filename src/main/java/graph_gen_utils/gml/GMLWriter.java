package graph_gen_utils.gml;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;

public abstract class GMLWriter {

	public abstract void write(GraphDatabaseService transNeo, File chacoFile);
}
