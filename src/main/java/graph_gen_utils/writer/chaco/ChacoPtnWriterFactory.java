package graph_gen_utils.writer.chaco;

import graph_gen_utils.NeoFromFile.ChacoType;
import graph_gen_utils.writer.GraphWriter;

import java.io.File;

public abstract class ChacoPtnWriterFactory {

	public static GraphWriter getChacoPtnWriter(ChacoType chacoType,
			File chacoFile, File ptnFile) throws Exception {

		switch (chacoType) {
		case UNWEIGHTED:
			return new ChacoPtnWriterUnweighted(chacoFile, ptnFile);
		case WEIGHTED_EDGES:
			throw new Exception("ChacoType[WEIGHTED_EDGES] not supported yet");
		case WEIGHTED_NODES:
			throw new Exception("ChacoType[WEIGHTED_NODES] not supported yet");
		case WEIGHTED:
			throw new Exception("ChacoType[WEIGHTED] not supported yet");
		default:
			throw new Exception("ChacoType not recognized");
		}

	}

}
