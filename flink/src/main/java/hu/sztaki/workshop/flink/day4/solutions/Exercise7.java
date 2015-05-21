package hu.sztaki.workshop.flink.day4.solutions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

public class Exercise7 {

	/**
	 * Exercise 7.<br>
	 * <br>
	 * Generate 100 random real numbers between 0 and 1. Print their sum, maximum, minimum.<br>
	 * Use aggregate.<br>
	 * <br>
	 * Hint:<br>
	 * Tuples should be used.<br>
	 * The sum should be around 50.<br>
	 * <br>
	 * {@link DataSet#sum}<br>
	 * {@link DataSet#aggregate}<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<Tuple1<Double>> randoms = env
				.generateSequence(1, 100)
				.map(new MapFunction<Long, Tuple1<Double>>() {
					public Tuple1<Double> map(Long value) throws Exception {
						return new Tuple1<Double>(Math.random());
					}
				});


		randoms.sum(0).print();
		randoms.max(0).print();
		randoms.min(0).print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}