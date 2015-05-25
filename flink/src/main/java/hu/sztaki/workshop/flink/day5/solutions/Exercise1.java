package hu.sztaki.workshop.flink.day5.solutions;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class Exercise1 {

	/**
	 * Exercise 0. Joining<br>
	 * <br>
	 * Output the fruit features that are assigned to a fruit with count.<br>
	 * Join the two DataSets based on the fruit names.<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSource<Tuple2<String, Integer>> fruitCount = env.fromElements(
				new Tuple2<String, Integer>("apple", 5),
				new Tuple2<String, Integer>("pear", 2),
				new Tuple2<String, Integer>("cherry", 3),
				new Tuple2<String, Integer>("banana", 1)
		);

		DataSource<Tuple2<String, String>> fruitFeature = env.fromElements(
				new Tuple2<String, String>("red", "cherry"),
				new Tuple2<String, String>("tasty", "apple")
		);

		fruitCount
				.join(fruitFeature)
				.where(0)
				.equalTo(1)
				.projectFirst(0)
				.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}