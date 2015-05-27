package hu.sztaki.workshop.flink.day8.solutions;

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

		DataStreamSource<String> listens = env.addSource(new ListenSource(500, 2000));
		DataStream<Long> listenTimes = (DataStream<Long>) listens.map(new MapFunction<String, Long>() {
			@Override
			public Long map(String listen) throws Exception {
				String[] split = listen.split("\t");
				String[] time = split[2].split(" ")[1].split(":");
				int hour = Integer.parseInt(time[0]);
				int min = Integer.parseInt(time[1]);
				int sec = Integer.parseInt(time[2]);
				return Long.valueOf(3600 * hour + 60 * min + sec);
			}
		});

		DataStream<Long> timeDifferences = listenTimes
				.window(Count.of(2))
				.every(Count.of(1))
				.mapWindow(new WindowMapFunction<Long, Long>() {
					@Override
					public void mapWindow(Iterable<Long> iterable, Collector<Long> collector) throws Exception {
						Iterator<Long> iterator = iterable.iterator();

						Long prev = iterator.hasNext() ? iterator.next() : null;
						Long current = iterator.hasNext() ? iterator.next() : null;

						if (prev != null && current != null) {
							collector.collect(current - prev);
						}
					}
				})
				.flatten();

		timeDifferences.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}