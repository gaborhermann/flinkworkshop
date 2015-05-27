package hu.sztaki.workshop.flink.day8.solutions;

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

		DataStreamSource<String> listens = env.addSource(new ListenSource(50, 400));

		DataStream<Integer> listensEverySecond = listens
				.window(Time.of(1, TimeUnit.SECONDS))
				.foldWindow(0, new FoldFunction<String, Integer>() {
					@Override
					public Integer fold(Integer listensInSecondSoFar, String o) throws Exception {
						return listensInSecondSoFar + 1;
					}
				})
				.flatten();

		listensEverySecond
//				.window(Count.of(1))
				.window(Delta.of(30, new DeltaFunction<Integer>() {
					@Override
					public double getDelta(Integer prevSec, Integer currentSec) {
						return Math.abs(currentSec - prevSec);
					}
				}, 0))
				.mapWindow(new WindowMapFunction<Integer, String>() {
					@Override
					public void mapWindow(Iterable<Integer> iterable, Collector<String> collector) throws Exception {
						StringBuilder sb = new StringBuilder("");
						for (Integer numOfListensInSec : iterable) {
							sb.append(numOfListensInSec);
							sb.append(' ');
						}
						collector.collect(sb.toString());
					}
				})
				.flatten()
				.print();

		// TODO code here

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}