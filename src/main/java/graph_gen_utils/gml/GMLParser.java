package graph_gen_utils.gml;

import java.io.File;
import java.io.FileNotFoundException;

import graph_gen_utils.general.NodeData;

public abstract class GMLParser {

	protected File gmlFile = null;

	public GMLParser(File gmlFile) throws FileNotFoundException {
		super();
		this.gmlFile = gmlFile;
	}

	public abstract Iterable<NodeData> getNodes();

	public abstract Iterable<NodeData> getRels();

}
