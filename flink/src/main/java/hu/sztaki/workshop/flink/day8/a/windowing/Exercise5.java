package hu.sztaki.workshop.flink.day8.a.windowing;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.util.Collector;

public class Exercise5 {

	/**
	 * Exercise 4. Eviction-trigger policy.
	 *
	 * Implement a count based sliding window using eviction and trigger policies
	 * with a window size of 3 and a slide size of 2.
	 * Test it on an integer stream.
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// TODO

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}