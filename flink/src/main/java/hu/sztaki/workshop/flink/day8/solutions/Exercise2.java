package hu.sztaki.workshop.flink.day8.solutions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Count;

import hu.sztaki.workshop.flink.utils.data.ListenSource;
import hu.sztaki.workshop.flink.utils.data.MockDataBaseConnection;

public class Exercise2 {

	/**
	 * Exercise 2.
	 *
	 * Using the static data set {@link MockDataBaseConnection}, determine the
	 * most active country the last 10 listen. Update the window every 4 listens
	 * using sliding window.
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		DataStream<Tuple3<String, String, String>> listens = (DataStream<Tuple3<String, String, String>>) env
				.addSource(new ListenSource(10, 300))
				.map(new MapFunction<String, Tuple3<String, String, String>>() {
					@Override
					public Tuple3<String, String, String> map(String listen) throws Exception {
						String[] split = listen.split("\t");
						return new Tuple3<String, String, String>(split[0], split[1], split[2]);
					}
				});

		DataStream<Tuple2<String, Integer>> countriesWithOne = (DataStream<Tuple2<String, Integer>>)
				listens
					.map(new RichMapFunction<Tuple3<String, String, String>, Tuple2<String, Integer>>() {
						private MockDataBaseConnection db;

						@Override
						public void open(Configuration parameters) throws Exception {
							db = new MockDataBaseConnection();
						}

						@Override
						public Tuple2<String, Integer> map(Tuple3<String, String, String> listen) throws Exception {
							String country = db.getCountryByUser(listen.f0);
							return new Tuple2<String, Integer>(country, 1);
						}
					});

		DataStream<Tuple2<String, Integer>> mostActiveCountry = countriesWithOne
				.window(Count.of(10))
				.groupBy(0)
				.sum(1)
				.maxBy(1)
				.flatten();

//		listens.print();
		mostActiveCountry.print();


		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}