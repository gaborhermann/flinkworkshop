package hu.sztaki.workshop.flink.day9;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

public class Exercise2 {

	/**
	 * Exercise 2.
	 *
	 * Parse a graph from CSV format (e.g. 'small-graph.csv').
	 * Print the vertex with the largest degree.
	 * Sort the vertices by degree and check the result.
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(1);

		String path = args[0];

		Graph<Long, NullValue, NullValue> graph = parseSmallGraph(path, env);

		// TODO code here

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Graph<Long, NullValue, NullValue> parseSmallGraph(String path, ExecutionEnvironment env) {
		// TODO code here
		return null;
	}
}