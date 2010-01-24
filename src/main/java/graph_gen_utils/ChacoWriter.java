package graph_gen_utils;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;

public abstract class ChacoWriter {

	public abstract void write(GraphDatabaseService transNeo, File chacoFile);
}
