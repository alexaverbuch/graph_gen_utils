package graph_gen_utils.writer;

import org.neo4j.graphdb.GraphDatabaseService;

public interface GraphWriter {
	public void write(GraphDatabaseService transNeo);
}
