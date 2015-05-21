package hu.sztaki.workshop.flink.day4;

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

		DataSource<String> words = env.fromCollection(input);

		DataSet<String> filteredWords = null; // TODO code here

		DataSink<String> print = filteredWords.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
