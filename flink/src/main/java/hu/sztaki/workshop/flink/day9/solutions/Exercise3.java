package hu.sztaki.workshop.flink.day9.solutions;

import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;

import hu.sztaki.workshop.flink.day9.util.GraphGenerator;

public class Exercise3 {

	/**
	 * Exercise 3.
	 * <p/>
	 * Create an undirected graph from the graph.
	 * Use the available graph transformations.
	 * Compare the result with {@link org.apache.flink.graph.Graph#getUndirected()}
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(1);

		Graph<Long, NullValue, NullValue> graph = GraphGenerator.getFixedSmallGraph(env);

		Graph<Long, NullValue, NullValue> expected = graph.getUndirected();

		try {
			Graph<Long, NullValue, NullValue> undirectedGraph = graph.run(new Undirected());

			List<Edge<Long, NullValue>> undirectedEgdes = undirectedGraph.getEdges().collect();
			List<Edge<Long, NullValue>> expectedEdges = expected.getEdges().collect();

			if (undirectedEgdes.equals(expectedEdges)) {
				System.out.println("Yeah, they are the same! :)");
			} else {
				System.out.println("Not our lucky day :(");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class Undirected implements GraphAlgorithm<Long, NullValue, NullValue> {

		@Override
		public Graph<Long, NullValue, NullValue> run(Graph<Long, NullValue, NullValue> graph) throws Exception {
			return graph.union(graph.reverse());
		}
	}
}