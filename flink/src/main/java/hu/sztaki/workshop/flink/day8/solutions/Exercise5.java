package hu.sztaki.workshop.flink.day8.solutions;

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

		DataStreamSource<Long> nums = env.generateSequence(1, 100);

		nums.window(
				new TriggerPolicy<Long>() {
					private int counter = 0;

					@Override
					public boolean notifyTrigger(Long num) {
						if (counter == 1) {
							counter = 0;
							return true;
						} else {
							counter++;
							return false;
						}
					}
				},
				new EvictionPolicy<Long>() {
					@Override
					public int notifyEviction(Long num, boolean UDFwasTriggered, int bufferSize) {
						if (!UDFwasTriggered) {
							return bufferSize - 2;
						} else {
							return 0;
						}
					}
				})
				.mapWindow(new WindowMapFunction<Long, String>() {
					@Override
					public void mapWindow(Iterable<Long> iterable, Collector<String> collector) throws Exception {
						StringBuilder sb = new StringBuilder("");
						for (Long numOfListensInSec : iterable) {
							sb.append(numOfListensInSec);
							sb.append(' ');
						}
						collector.collect(sb.toString());
					}
				})
		.flatten().print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}