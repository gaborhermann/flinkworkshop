package hu.sztaki.workshop.flink.day4;

import java.util.Arrays;
import java.util.Collection;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

public class Exercise2 {

	/**
	 * Exercise 2.
	 *
	 * Print only the words starting with 'F' from the given input collection.
	 *
	 * Hints:
	 * Create local ExecutionEnvironment with parallelism 1, use flatMap.
	 *
	 * {@link org.apache.flink.api.java.ExecutionEnvironment#fromCollection}
	 * {@link org.apache.flink.api.java.DataSet#flatMap}
	 */
	public static void main(String[] args) {
		Collection<String> input = Arrays.asList("Apache", "Flink", "workshop", "Fun");

		// TODO code here
	}
}
