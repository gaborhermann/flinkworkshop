package hu.sztaki.workshop.flink.day4;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;

public class Exercise1 {

	/**
	 * Exercise 1.
	 *
	 * Output numbers from 100 to 200 with an exclamation mark (e.g. !132).
	 *
	 * Hints:
	 *
	 * Use the local environment to generate numbers from 100 to 200.
	 * Map them to a String beginning with '!'.
	 * Print it.
	 * Execute the environment.
	 *
	 * {@link org.apache.flink.api.java.ExecutionEnvironment#generateSequence}
	 * {@link org.apache.flink.api.java.DataSet#map}
	 * {@link org.apache.flink.api.java.DataSet#print}
	 * {@link org.apache.flink.api.java.ExecutionEnvironment#execute}
	 */
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();


		// TODO code here

		DataSet<Integer> integerDataSource = env.fromElements(1, 2, 3, 7123);

		integerDataSource.map(x -> x + 2).print();

		env.execute();


	}

}
