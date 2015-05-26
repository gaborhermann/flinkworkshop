package hu.sztaki.workshop.flink.day7.streaming.intro;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;

public class Exercise2 {

	/**
	 * Exercise 2.<br>
	 * <br>
	 * Create a stream that continuously emits random numbers between 0 and 1.<br>
	 * Wait a millisecond between every emission.<br>
	 * Filter the ones larger than 0.5.<br>
	 * <br>
	 * Hint:<br>
	 * {@link StreamExecutionEnvironment#addSource(SourceFunction)}<br>
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// TODO code here

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}