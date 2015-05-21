package hu.sztaki.workshop.flink.day4.solutions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class Exercise5 {

	/**
	 * Exercise 5.<br>
	 * <br>
	 * Print positive integers from 1 to 20 with their squares.<br>
	 * Use Tuple2<Long, Long><br>
	 * Use parallelism of 3.<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(3);

		env
				.generateSequence(1, 20)
				.map(new MapFunction<Long, Tuple2<Long, Long>>() {
					public Tuple2<Long, Long> map(Long value) throws Exception {
						return new Tuple2<Long, Long>(value, value * value);
					}
				})
				.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
