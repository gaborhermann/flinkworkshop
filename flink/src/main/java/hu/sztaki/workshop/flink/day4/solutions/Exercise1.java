package hu.sztaki.workshop.flink.day4.solutions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;

public class Exercise1 {

	/**
	 * Exercise 1.<br>
	 * <br>
	 * Output numbers from 100 to 200 with an exclamation mark (e.g. !132).<br>
	 * <br>
	 * Hints:<br>
	 * <br>
	 * Use the local environment to generate numbers from 100 to 200.<br>
	 * Map them to a String beginning with '!'.<br>
	 * Print it.<br>
	 * Execute the environment.<br>
	 * <br>
	 * {@link ExecutionEnvironment#generateSequence}<br>
	 * {@link DataSet#map}<br>
	 * {@link DataSet#print}<br>
	 * {@link ExecutionEnvironment#execute}<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(1);

		DataSource<Long> numbers = env.generateSequence(100, 200);

		DataSet<String> numbersWithExclamationMark = numbers.map(
				new MapFunction<Long, String>() {
					public String map(Long value) throws Exception {
						return "!" + value;
					}
				}
		);

		DataSink<String> print = numbersWithExclamationMark.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
