package graph_gen_utils.gml;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Scanner;

import graph_gen_utils.general.NodeData;

public abstract class GMLParser implements Iterator<NodeData> {

	Scanner gmlScanner = null;

	public GMLParser(File gmlFile) throws FileNotFoundException {
		super();
		this.gmlScanner = new Scanner(gmlFile);
	}
	
}
