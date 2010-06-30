package graph_gen_utils.writer.chaco;

import graph_gen_utils.general.ChacoType;
import graph_gen_utils.writer.GraphWriter;

import java.io.File;

public abstract class ChacoWriterFactory {

	public static GraphWriter getChacoWriter(ChacoType chacoType, File chacoFile)
			throws Exception {

		switch (chacoType) {
		case UNWEIGHTED:
			return new ChacoWriterUnweighted(chacoFile);
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
