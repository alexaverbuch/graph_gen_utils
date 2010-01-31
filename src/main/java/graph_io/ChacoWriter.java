package graph_io;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;

public abstract class ChacoWriter {

	public abstract void write(GraphDatabaseService transNeo, File chacoFile);
	
	public abstract void write(GraphDatabaseService transNeo, File chacoFile, File ptnFile);
}
