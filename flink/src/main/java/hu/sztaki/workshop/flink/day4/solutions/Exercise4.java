package hu.sztaki.workshop.flink.day4.solutions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

public class Exercise4 {

	/**
	 * Exercise 4.<br>
	 * <br>
	 * Print the words in the lines starting with<br>
	 * an exclamation mark (every word in a line).<br>
	 * <br>
	 * Hint:<br>
	 * Use flatMap<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSource<String> lines = env.fromElements("Apache Flink es guay", "Me gusta las ardillas");

		DataSet<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public void flatMap(String value, Collector<String> out) throws Exception {
				for (String word : value.split(" ")) {
					out.collect("!" + word);
				}
			}
		});

		words.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
