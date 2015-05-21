package hu.sztaki.workshop.flink.day4.solutions;

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
	 * Exercise 2.<br>
	 * <br>
	 * Print only the words starting with 'F' from the given input collection.<br>
	 * <br>
	 * Hints:<br>
	 * Create local ExecutionEnvironment with parallelism 1, use flatMap.<br>
	 * <br>
	 * {@link ExecutionEnvironment#fromCollection}<br>
	 * {@link DataSet#flatMap}<br>
	 */
	public static void main(String[] args) {
		Collection<String> input = Arrays.asList("Apache", "Flink", "workshop", "Fun");

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(1);
		DataSource<String> words = env.fromCollection(input);

		final DataSet<String> filteredWords = words.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				if (value.charAt(0) == 'F') {
					out.collect(value);
				}
			}
		});

		DataSink<String> print = filteredWords.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
