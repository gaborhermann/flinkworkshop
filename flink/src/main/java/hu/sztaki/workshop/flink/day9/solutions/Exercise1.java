package hu.sztaki.workshop.flink.day9.solutions;

import java.util.Arrays;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

public class Exercise1 {

	/**
	 * Exercise 1.
	 *
	 * Create an example graph with 8 edges.
	 * Print the edges.
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		Graph.fromCollection(Arrays.asList(
				new Edge<Long, NullValue>(1L, 2L, NullValue.getInstance()),
				new Edge<Long, NullValue>(1L, 4L, NullValue.getInstance()),
				new Edge<Long, NullValue>(2L, 4L, NullValue.getInstance()),
				new Edge<Long, NullValue>(3L, 2L, NullValue.getInstance()),
				new Edge<Long, NullValue>(4L, 5L, NullValue.getInstance()),
				new Edge<Long, NullValue>(4L, 5L, NullValue.getInstance()),
				new Edge<Long, NullValue>(5L, 6L, NullValue.getInstance()),
				new Edge<Long, NullValue>(6L, 2L, NullValue.getInstance())
		), env);

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}