package hu.sztaki.workshop.flink.day4;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Exercise7 {

	/**
	 * Exercise 7.<br>
	 * <br>
	 * Generate 100 random real numbers between 0 and 1. Print their sum, maximum, minimum.<br>
	 * Use aggregate.<br>
	 * <br>
	 * Hint:<br>
	 * Tuples should be used.<br>
	 * The sum should be around 50.<br>
	 * <br>
	 * {@link DataSet#sum}<br>
	 * {@link DataSet#aggregate}<br>
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