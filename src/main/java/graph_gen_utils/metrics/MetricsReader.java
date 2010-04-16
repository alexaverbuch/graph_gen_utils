package graph_gen_utils.metrics;

public interface MetricsReader {

	public long nodeCount();

	public long edgeCount();

	public long edgeCut();

	public double edgeCutPerc();

	public long clusterSizeDiff();

	public double clusterSizeDiffPerc();

	public long clusterCount();

	public long meanClusterSize();

	public long minClusterSize();

	public long maxClusterSize();

	public double modularity();

	public double clusteringCoefficient();

	public String clustersToString();

}
