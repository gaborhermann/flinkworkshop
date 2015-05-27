package hu.sztaki.workshop.flink.day8.solutions;

import org.apache.flink.api.java.ExecutionEnvironment;

public class Exercise4 {

	/**
	 * Exercise 4.
	 *
	 * Determine the oldest user the last ten seconds using FullStream.
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