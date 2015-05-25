package hu.sztaki.workshop.flink.day5.solutions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class Exercise5 {

	/**
	 * Exercise 4.<br>
	 * <br>
	 * Use iteration to increase numbers by 100 (by 1 in every iteration).<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6);

		IterativeDataSet<Integer> iterate = source.iterate(100);

		DataSet<Integer> iterationStep = iterate.map(x -> x + 1);

		iterate
				.closeWith(iterationStep)
				.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}