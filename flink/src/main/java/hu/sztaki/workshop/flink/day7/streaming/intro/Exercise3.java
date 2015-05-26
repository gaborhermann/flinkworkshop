package hu.sztaki.workshop.flink.day7.streaming.intro;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class Exercise3 {

	/**
	 * Exercise 3.<br>
	 * <br>
	 * Connect to a Kafka topic, use consume producer to send messages, print them.<br>
	 * <br>
	 * Use:<br>
	 * {@link KafkaSource}<br>
	 * {@link SimpleStringSchema}<br>
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		String defaultZKAddress = "localhost:2181";

		// TODO code here

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}