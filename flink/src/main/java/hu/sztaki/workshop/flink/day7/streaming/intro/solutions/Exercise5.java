package hu.sztaki.workshop.flink.day7.streaming.intro.solutions;

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

		env
				.generateSequence(1, 10).setParallelism(1).broadcast()
				.filter(x -> x % 2 == 0).setParallelism(2)
				.print().setParallelism(2);

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}