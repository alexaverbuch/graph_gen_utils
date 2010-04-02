package graph_gen_utils.writer.gml;

import graph_gen_utils.writer.GraphWriter;

import java.io.File;

public abstract class GMLWriter implements GraphWriter {

	protected File gmlFile = null;

	public GMLWriter(File gmlFile) {
		this.gmlFile = gmlFile;
	}

	protected String valueToStr(Object value) {

		if ((value instanceof Byte) || (value instanceof Integer)
				|| (value instanceof Long) || (value instanceof Float)
				|| (value instanceof Double) || (value instanceof Boolean))
			return value.toString();

		if (value instanceof String)
			return String.format("\"%s\"", value);

		return String.format("\"%s\"", value);

	}

}
