package hu.sztaki.workshop.flink.day8.a.windowing;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Time;

import hu.sztaki.workshop.flink.utils.data.ListenSource;

public class Exercise1 {

	/**
	 * Exercise 1.
	 *
	 * Use {@link ListenSource} as source with 10 minimum and 500 maximum wait time.
	 * Create a window of 5 seconds, count the elements every window.
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);

		// TODO code here

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}