package hu.sztaki.workshop.flink.day8.b.window.join;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.SystemTimestamp;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;

import hu.sztaki.workshop.flink.utils.data.ListenSource;
import hu.sztaki.workshop.flink.utils.data.RadioSource;

public class Exercise7 {

	/**
	 * Exercise 7.<br>
	 * <br>
	 * Mark the songs that are played on the radio and a user<br>
	 * listens to it within 1 second. Use window join.<br>
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// TODO

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}