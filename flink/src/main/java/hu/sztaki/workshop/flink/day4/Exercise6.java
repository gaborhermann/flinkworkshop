package hu.sztaki.workshop.flink.day4;

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

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
