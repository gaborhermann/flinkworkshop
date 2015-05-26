package hu.sztaki.workshop.flink.day7.streaming.intro;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class Exercise4 {

	/**
	 * Exercise 4.<br>
	 * <br>
	 * Create two Kafka topics called 'numbers' and 'sums'.<br>
	 * Use console producer to produce integers to 'numbers'.<br>
	 * Parse them with Flink and get the current sum.<br>
	 * Produce it to the 'sums' topic.<br>
	 * Write your own serialization schema with {@link String#getBytes()}.<br>
	 * <br>
	 * Use:<br>
	 * {@link DataStream#sum(int)}<br>
	 * {@link KafkaSink}<br>
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		String defaultZKAddress = "localhost:2181";
		String numbersTopic = "numbers";
		String sumsTopic = "sums";

		// TODO code here

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}