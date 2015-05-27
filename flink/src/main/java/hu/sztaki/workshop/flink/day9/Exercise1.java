package hu.sztaki.workshop.flink.day9;

import org.apache.flink.api.java.ExecutionEnvironment;

public class Exercise1 {

	/**
	 * Exercise 1.
	 *
	 * Create an example graph with 7 edges.
	 * Print the edges.
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