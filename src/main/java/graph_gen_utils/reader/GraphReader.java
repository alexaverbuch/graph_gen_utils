package graph_gen_utils.reader;

import graph_gen_utils.general.NodeData;

public interface GraphReader {

	public Iterable<NodeData> getNodes();

	public Iterable<NodeData> getRels();

}
