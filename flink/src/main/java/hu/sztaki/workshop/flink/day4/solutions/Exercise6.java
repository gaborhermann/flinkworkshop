package hu.sztaki.workshop.flink.day4.solutions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Exercise6 {

	/**
	 * Exercise 6.<br>
	 * <br>
	 * Sum the numbers from 1 to 100.<br>
	 * Use reduce.<br>
	 * <br>
	 * Hint:<br>
	 * The result must be (101 * 100) / 2 = 5050.<br>
	 * <br>
	 * {@link DataSet#reduce}<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		env
				.generateSequence(1, 100)
				.reduce(new ReduceFunction<Long>() {
					public Long reduce(Long value1, Long value2) throws Exception {
						return value1 + value2;
					}
				})
				.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
