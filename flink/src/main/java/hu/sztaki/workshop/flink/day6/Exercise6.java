package hu.sztaki.workshop.flink.day6;

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

		// TODO code here

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}