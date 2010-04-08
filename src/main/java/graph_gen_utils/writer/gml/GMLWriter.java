package graph_gen_utils.writer.gml;

import graph_gen_utils.writer.GraphWriter;

import java.io.File;

public abstract class GMLWriter implements GraphWriter {

	protected File gmlFile = null;

	public GMLWriter(File gmlFile) {
		this.gmlFile = gmlFile;
	}

	protected String valToStr(Object value) {

		if ((value instanceof Byte) || (value instanceof Integer)
				|| (value instanceof Long) || (value instanceof Float)
				|| (value instanceof Double) || (value instanceof Boolean))
			return value.toString();

		if (value instanceof String)
			return String.format("\"%s\"",
					removeIllegalValChars((String) value));

		return String.format("\"%s\"", removeIllegalValChars(value.toString()));

	}

	protected String removeIllegalKeyChars(String key) {
		String regex = "[^a-zA-Z_]";
		return key.replaceAll(regex, "");
	}

	protected String removeIllegalValChars(String val) {
		String regex = "[^0-9a-zA-Z_\\s-]";
		return val.replaceAll(regex, " ");
	}
}
