package dodgy_tests;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ChacoType;
import graph_gen_utils.general.Consts;
import graph_gen_utils.general.DirUtils;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.memory_graph.MemNode;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class DodgyTests {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			read_write_read_write_etc();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void read_write_read_write_etc() throws Exception {
		DirUtils.cleanDir("var");

		DirUtils.cleanDir("var/neo0");
		GraphDatabaseService neo0 = new EmbeddedGraphDatabase("var/neo0");
		NeoFromFile.writeNeoFromChaco(neo0, "graphs/test0.graph");
		NeoFromFile.writeChaco(neo0, "var/neo0.graph", ChacoType.UNWEIGHTED);
		NeoFromFile.writeGMLBasic(neo0, "var/neo0.basic.gml");
		MemGraph mem0 = NeoFromFile.readMemGraph(neo0);
		NeoFromFile.writeChaco(mem0, "var/neo0mem00.graph",
				ChacoType.UNWEIGHTED);
		NeoFromFile.writeGMLBasic(mem0, "var/neo0mem00.basic.gml");

		Transaction tx1 = mem0.beginTx();
		try {
			mem0.setNextNodeId(9);
			MemNode memNode9 = (MemNode) mem0.createNode();

			memNode9.setNextRelId(9);
			memNode9.createRelationshipTo(mem0.getNodeById(5),
					DynamicRelationshipType
							.withName(Consts.DEFAULT_REL_TYPE_STR));

			((MemNode) mem0.getNodeById(5)).setNextRelId(10);
			mem0.getNodeById(5).createRelationshipTo(
					memNode9,
					DynamicRelationshipType
							.withName(Consts.DEFAULT_REL_TYPE_STR));

			((MemNode) mem0.getNodeById(5)).setNextRelId(11);
			mem0.getNodeById(5).createRelationshipTo(
					memNode9,
					DynamicRelationshipType
							.withName(Consts.DEFAULT_REL_TYPE_STR));

			NeoFromFile.writeGMLBasic(mem0, "var/neo0mem01.basic.gml");
			NeoFromFile.writeChaco(mem0, "var/neo0mem01.graph",
					ChacoType.UNWEIGHTED);

			tx1.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx1.finish();
		}

		DirUtils.cleanDir("var/neo1");
		GraphDatabaseService neo1 = new EmbeddedGraphDatabase("var/neo1");
		NeoFromFile.writeNeoFromGML(neo1, "var/neo0mem01.basic.gml");
		MemGraph mem1 = NeoFromFile.readMemGraph(neo1);
		NeoFromFile.writeGMLBasic(mem1, "var/neo1mem10.basic.gml");
		NeoFromFile.writeChaco(mem1, "var/neo1mem10.graph",
				ChacoType.UNWEIGHTED);

		Transaction tx2 = mem1.beginTx();
		try {

			Relationship memRel = mem1.getRelationshipById(3);
			System.out.printf("***Deleting MemRel[%d][%d->%d]***\n", memRel
					.getId(), memRel.getStartNode().getId(), memRel
					.getEndNode().getId());
			memRel.delete();

			tx2.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx2.finish();
		}

		NeoFromFile.writeGMLBasic(mem1, "var/neo1mem11.basic.gml");
		NeoFromFile.writeChaco(mem1, "var/neo1mem11.graph",
				ChacoType.UNWEIGHTED);

		Transaction tx3 = mem1.beginTx();
		try {
			MemNode memNode5 = (MemNode) mem1.getNodeById(5);
			for (Relationship memRel : memNode5
					.getRelationships(Direction.OUTGOING)) {
				System.out.printf("***MemNode[%d] MemRel[%d->%d]***\n",
						memNode5.getId(), memRel.getStartNode().getId(), memRel
								.getEndNode().getId());
			}
			System.out.println();
			for (Relationship memRel : memNode5
					.getRelationships(Direction.INCOMING)) {
				System.out.printf("***MemNode[%d] MemRel[%d<-%d]***\n",
						memNode5.getId(), memRel.getEndNode().getId(), memRel
								.getStartNode().getId());
			}
			System.out.println();
			for (Relationship memRel : memNode5
					.getRelationships(Direction.BOTH)) {
				System.out.printf("***MemNode[%d] MemRel[%d-%d]***\n", memNode5
						.getId(), memRel.getStartNode().getId(), memRel
						.getEndNode().getId());
			}
			System.out.println();
			for (Relationship memRel : memNode5.getRelationships()) {
				System.out.printf("***MemNode[%d] MemRel[%d-%d]***\n", memNode5
						.getId(), memRel.getStartNode().getId(), memRel
						.getEndNode().getId());
			}

			tx3.success();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx3.finish();
		}

		DirUtils.cleanDir("var/neo2");
		GraphDatabaseService neo2 = new EmbeddedGraphDatabase("var/neo2");
		NeoFromFile.writeNeoFromGML(neo2, "var/neo1mem11.basic.gml");
		MemGraph mem2 = NeoFromFile.readMemGraph(neo2);
		NeoFromFile.writeGMLBasic(mem2, "var/neo2mem00.basic.gml");
		NeoFromFile.writeChaco(mem2, "var/neo2mem00.graph",
				ChacoType.UNWEIGHTED);

		neo0.shutdown();
		neo1.shutdown();
		neo2.shutdown();
	}

}
