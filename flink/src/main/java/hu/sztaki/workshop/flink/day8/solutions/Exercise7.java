package hu.sztaki.workshop.flink.day8.solutions;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

		DataStreamSource<String> radio = env.addSource(new RadioSource(10, 500));

		DataStream<Tuple2<String, String>> parsedRadio = (DataStream<Tuple2<String, String>>) radio.map(new MapFunction<String, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> map(String s) throws Exception {
				String[] split = s.split("\t");
				return new Tuple2<String, String>(split[0], split[1]);
			}
		});


		DataStreamSource<String> listens = env.addSource(new ListenSource(10, 500));
		DataStream<Tuple3<String, String, String>> parsedListens = (DataStream<Tuple3<String, String, String>>) listens.map(new MapFunction<String, Tuple3<String, String, String>>() {
			@Override
			public Tuple3<String, String, String> map(String s) throws Exception {
				String[] split = s.split("\t");
				return new Tuple3<String, String, String>(split[0], split[1], split[2]);
			}
		});

		listens.print();
		parsedRadio
				.join(parsedListens)
				.onWindow(1, TimeUnit.SECONDS)
				.where(0)
				.equalTo(1)
				.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}