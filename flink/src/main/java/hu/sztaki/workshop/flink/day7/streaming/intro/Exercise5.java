package hu.sztaki.workshop.flink.day7.streaming.intro;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Exercise5 {

	/**
	 * Exercise 5.<br>
	 * <br>
	 * Generate a sequence with parallelism 1.<br>
	 * Set partitioning to broadcast.<br>
	 * Filter the even numbers with parallelism 2.<br>
	 * Print the result also with parallelism 2.<br>
	 * <br>
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// TODO code here

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}