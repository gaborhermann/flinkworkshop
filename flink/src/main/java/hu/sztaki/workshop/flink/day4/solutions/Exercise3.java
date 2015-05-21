package hu.sztaki.workshop.flink.day4.solutions;

import java.util.Arrays;
import java.util.Collection;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;

public class Exercise3 {

	/**
	 * Exercise 3.<br>
	 * <br>
	 * Print only the words starting with 'F' from the given input collection.<br>
	 * Set parallelism to 3.<br>
	 * <br>
	 * Hints:<br>
	 * Use filter.<br>
	 * <br>
	 * {@link DataSet#filter}<br>
	 * {@link ExecutionEnvironment#setParallelism(int)}<br>
	 */
	public static void main(String[] args) {
		Collection<String> input = Arrays.asList("Apache", "Flink", "workshop", "Fun");

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(3);

		DataSource<String> words = env.fromCollection(input);

		DataSet<String> filteredWords = words.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				return value.charAt(0) == 'F';
			}
		}).setParallelism(3);

		DataSink<String> print = filteredWords.print().setParallelism(3);

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
