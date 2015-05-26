package hu.sztaki.workshop.flink.day7.streaming.intro;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Exercise1 {

	/**
	 * Exercise 1.<br>
	 * <br>
	 * Output numbers from 100 to 200 with an exclamation mark (e.g. !132).<br>
	 * <br>
	 * Hints:<br>
	 * <br>
	 * Use the local environment to generate numbers from 100 to 200.<br>
	 * Map them to a String beginning with '!'.<br>
	 * Print it.<br>
	 * Execute the environment.<br>
	 * <br>
	 * {@link StreamExecutionEnvironment#generateSequence}<br>
	 * {@link DataStream#map}<br>
	 * {@link DataStream#print}<br>
	 * {@link StreamExecutionEnvironment#execute}<br>
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		DataStreamSource<Long> numbers = env.generateSequence(100, 200);

		DataStream<String> numbersWithExclamationMark = (DataStream<String>) numbers.map(new MapFunction<Long, String>() {
			@Override
			public String map(Long num) throws Exception {
				return "!" + num;
			}
		});

		DataStreamSink<String> print = numbersWithExclamationMark.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}