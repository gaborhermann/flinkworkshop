package hu.sztaki.workshop.flink.day6.solutions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Exercise6 {

	/**
	 * Exercise 6.<br>
	 * <br>
	 * Write a simple job (it could be anything).<br>
	 * and follow the execution lifecycle of a job.<br>
	 * <br>
	 * Throw an exception and see what happens.<br>
	 * <br>
	 * Now set the number of execution retries to 2 and see what happens<br>
	 * <br>
	 * Use<br>
	 * {@link ExecutionEnvironment#setNumberOfExecutionRetries(int)}<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		env.setNumberOfExecutionRetries(2);

		env.fromElements(1, 2, 3).map(new MapFunction<Integer, Integer>() {
			public Integer map(Integer integer) throws Exception {
				if (integer == 2) {
					throw new Exception("FAIL THIS JOB!!!");
				}
				return integer;
			}
		}).print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}