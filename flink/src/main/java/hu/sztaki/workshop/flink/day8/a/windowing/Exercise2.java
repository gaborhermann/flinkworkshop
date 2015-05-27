package hu.sztaki.workshop.flink.day8.a.windowing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Count;

import hu.sztaki.workshop.flink.utils.data.ListenSource;
import hu.sztaki.workshop.flink.utils.data.MockDataBaseConnection;

public class Exercise2 {

	/**
	 * Exercise 2.
	 *
	 * Using the static data set {@link MockDataBaseConnection}, determine the
	 * most active country the last 10 listen. Update the window every 4 listens
	 * using sliding window.
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