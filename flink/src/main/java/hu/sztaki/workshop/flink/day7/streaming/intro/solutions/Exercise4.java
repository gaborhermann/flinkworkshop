package hu.sztaki.workshop.flink.day7.streaming.intro.solutions;

import org.apache.flink.api.common.functions.MapFunction;
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

		env
				.addSource(new KafkaSource<String>(defaultZKAddress, numbersTopic, new SimpleStringSchema()))
				.map(new MapFunction<String, Tuple1<Integer>>() {
					@Override
					public Tuple1<Integer> map(String s) throws Exception {
						return new Tuple1<Integer>(Integer.parseInt(s));
					}
				})
				.sum(0)
				.map(new MapFunction<Tuple1<Integer>, String>() {
					@Override
					public String map(Tuple1<Integer> sum) throws Exception {
						return sum.toString();
					}
				})
				.addSink(new KafkaSink<String>(defaultZKAddress,
						sumsTopic,
						new SerializationSchema<String, byte[]>() {
							@Override
							public byte[] serialize(String s) {
								return s.getBytes();
							}
						}));

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}