package hu.sztaki.workshop.flink.day5.solutions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Exercise0 {

	/**
	 * Exercise 0. Implement WordCount in Flink.<br>
	 * Try using a HDFS file as input.<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		env.fromElements("a b c a a", "b b a", "a c")
				// splitting
				.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
						String[] words = line.split(" ");
						for (String word : words) {
							out.collect(new Tuple2<>(word, 1));
						}
					}
				})
				// counting by words
				.groupBy(0)
				.sum(1)
				// output
				.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}