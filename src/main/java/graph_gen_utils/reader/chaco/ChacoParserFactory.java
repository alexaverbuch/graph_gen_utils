package graph_gen_utils.reader.chaco;

import graph_gen_utils.reader.GraphReader;

import java.io.File;
import java.util.Scanner;
import java.util.StringTokenizer;

public abstract class ChacoParserFactory {

	public static GraphReader getChacoParser(String graphPath) throws Exception {

		File graphFile = new File(graphPath);

		Scanner scanner = new Scanner(graphFile);

		// read first line to distinguish format
		StringTokenizer st = new StringTokenizer(scanner.nextLine(), " ");

		scanner.close();

		int nodeCount = 0;
		int edgeCount = 0;
		int format = 0;

		if (st.hasMoreTokens()) {
			nodeCount = Integer.parseInt(st.nextToken());
		}

		if (st.hasMoreTokens()) {
			edgeCount = Integer.parseInt(st.nextToken());
		}

		if (st.hasMoreTokens()) {
			format = Integer.parseInt(st.nextToken());
		}

		System.out.printf("Graph Properties:%n");
		System.out.printf("\tNodes \t= %d%n", nodeCount);
		System.out.printf("\tEdges \t= %d%n", edgeCount);

		switch (format) {
		case 0:
			System.out.printf("\tFormat \t= Unweighted%n");
			return new ChacoParserUnweighted(graphFile);
		case 1:
			System.out.printf("\tFormat \t= Weighted Edges%n");
			return new ChacoParserWeightedEdges(graphFile);
		case 10:
			System.out.printf("\tFormat \t= Weighted Nodes%n");
			return new ChacoParserWeightedNodes(graphFile);
		case 11:
			System.out.printf("\tFormat \t= Weighted%n");
			return new ChacoParserWeighted(graphFile);
		default:
			throw new Exception("Unrecognized Chaco Format");
		}
	}

}
