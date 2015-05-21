package hu.sztaki.workshop.flink.day4.solutions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class Exercise8_PiEstimation {

	/**
	 * Exercise 8.<br>
	 * <br>
	 * Implement the Monte Carlo method for pi estimation.<br>
	 * Get the error of the estimation with Math.PI.<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(2);

		long numberOfSamples = 1000000L;

		DataSet<Tuple2<Double, Double>> randoms = env
				.generateSequence(1, numberOfSamples)
				.map(new MapFunction<Long, Tuple2<Double, Double>>() {
					public Tuple2<Double, Double> map(Long value) throws Exception {
						return new Tuple2<Double, Double>(Math.random(), Math.random());
					}
				});

		try {
			long numberOfSamplesInCircle = randoms.filter(new FilterFunction<Tuple2<Double, Double>>() {
				public boolean filter(Tuple2<Double, Double> value) throws Exception {
					// x*x + y*y <= 1
					return value.f0 * value.f0 + value.f1 * value.f1 <= 1;
				}
			}).count();

			double pi = 4 * (double) numberOfSamplesInCircle / numberOfSamples;
			System.out.println("pi = " + pi);
			System.out.println("error = " + Math.abs(Math.PI - pi));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
