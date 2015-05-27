package hu.sztaki.workshop.flink.day8.a.windowing;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
import org.apache.flink.streaming.api.windowing.helper.Delta;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.util.Collector;

import hu.sztaki.workshop.flink.utils.data.ListenSource;

public class Exercise6 {

	/**
	 * Exercise 6.
	 *
	 * Count the number of listens every second.
	 *
	 * Determine the spikes using delta policy.
	 *
	 * Print whole windows using mapWindow and a StringBuilder.
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