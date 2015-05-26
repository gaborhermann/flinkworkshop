package hu.sztaki.workshop.flink.day7.streaming.intro.solutions;

import java.util.Random;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

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
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		DataStreamSource<Double> randomSource = env
				.addSource(new SourceFunction<Double>() {
//					THIS IS WRONG!
//					@Override
//					public void run(Collector<Double> collector) throws Exception {
//						Random rand = new Random();
//
//						while (true) {
//							collector.collect(rand.nextDouble());
//							Thread.sleep(1);
//						}
//					}
//
//					@Override
//					public void cancel() {
//					}

					private boolean isRunning = false;

					@Override
					public void run(Collector<Double> collector) throws Exception {
						Random rand = new Random();

						isRunning = true;

						while (isRunning) {
							collector.collect(rand.nextDouble());
							Thread.sleep(1);
						}
					}

					@Override
					public void cancel() {
						isRunning = false;
					}
				});

		randomSource
				.filter(new FilterFunction<Double>() {
					@Override
					public boolean filter(Double num) throws Exception {
						if (num < 0.01) {
							throw new Exception("Cancel job!");
						}
						return num > 0.5;
					}
				})
				.print();


		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}