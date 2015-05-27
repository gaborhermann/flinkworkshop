package hu.sztaki.workshop.flink.day8.a.windowing;

import java.util.Iterator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

import hu.sztaki.workshop.flink.utils.data.ListenSource;

public class  Exercise3 {

	/**
	 * Exercise 3.
	 *
	 * Determine the time elapsed between listens.
	 * Hint:
	 * Use a count based window of 2.
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