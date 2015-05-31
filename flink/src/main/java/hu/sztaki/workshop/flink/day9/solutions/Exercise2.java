package hu.sztaki.workshop.flink.day9.solutions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

public class Exercise2 {

	/**
	 * Exercise 2.
	 *
	 * Parse the 'small-graph.csv' as a graph.
	 * Print the vertex with the largest degree.
	 * Sort the vertices by degree and check the result.
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(1);

		String path = args[0];

		Graph<Long, NullValue, NullValue> graph = parseSmallGraph(path, env);

		DataSet<Tuple2<Long, Long>> degrees = graph.getDegrees();

		degrees.maxBy(1, 0).print();

		degrees.sortPartition(1, Order.DESCENDING).first(1).print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Graph<Long, NullValue, NullValue> parseSmallGraph(String path, ExecutionEnvironment env) {
		DataSet<Edge<Long, NullValue>> edgeDataSet = env.readCsvFile(path)
				.fieldDelimiter(" ")
				.lineDelimiter("\n")
				.types(Long.class, Long.class)
				.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
					@Override
					public Edge<Long, NullValue> map(Tuple2<Long, Long> tuple) throws Exception {
						return new Edge<Long, NullValue>(tuple.f0, tuple.f1, NullValue.getInstance());
					}
				});
		return Graph.fromDataSet(edgeDataSet, env);
	}
}