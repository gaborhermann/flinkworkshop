package hu.sztaki.workshop.flink.day4;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class Exercise4 {

	/**
	 * Exercise 4.<br>
	 * <br>
	 * Print the words from the lines.<br>
	 * Make each printed word start with an exclamation mark.<br>
	 * <br>
	 * Hint:<br>
	 * Use flatMap<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSource<String> lines = env.fromElements("Apache Flink es guay", "Me gusta las ardillas");

		// TODO code here
	}
}
