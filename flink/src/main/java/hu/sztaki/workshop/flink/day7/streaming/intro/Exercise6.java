package hu.sztaki.workshop.flink.day7.streaming.intro;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public class Exercise6 {

	/**
	 * Exercise 6.<br>
	 * <br>
	 * Measure the latency for every 500 element by transforming<br>
	 * the stream and printing the result.<br>
	 * <br>
	 * Try modifying the buffer timeout and see the results<br>
	 * (e.g. set to 1 milliseconds then 1000 milliseconds).<br>
	 * Also try turning on chaining. Simply do not set chaining strategy.<br>
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		env
				.generateSequence(1, 100000000)
					.setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER)
				.map(new MapFunction<Long, Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Long> map(Long n) throws Exception {
						Thread.sleep(1);
						return new Tuple2<>(n, System.currentTimeMillis());
					}
				})
					.setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER)
				.filter((Tuple2<Long, Long> t2) -> {
					return t2.f0 % 500 == 0;
				})
					.setChainingStrategy(StreamInvokable.ChainingStrategy.NEVER);

				// TODO add sink that measures latency

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}