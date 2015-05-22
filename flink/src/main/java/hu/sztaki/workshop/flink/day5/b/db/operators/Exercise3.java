package hu.sztaki.workshop.flink.day5.b.db.operators;

import java.io.Serializable;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class Exercise3 {

	/**
	 * Exercise 2.<br>
	 * <br>
	 * Give a list of the songs listened to by users from Hungary.<br>
	 * Using listens.csv and users.csv.<br>
	 * <br>
	 * Filter the users from Hungary.<br>
	 * Join the users and listens based on the user ids.<br>
	 * Use KeySelectors.<br>
	 * Project the songs, and make sure every song is only seen once.<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		CsvReader listensCsv = standardCsv(env, args[0]);
		DataSource<Listens> listens = listensCsv
				.pojoType(Listens.class, new String[]{"name", "song", "timeOfListen"});

		CsvReader usersCsv = standardCsv(env, args[1]);
		DataSet<Exercise2.User> usersPojo = usersCsv
				.pojoType(Exercise2.User.class, new String[]{"name", "country", "dateOfBirth"});

		usersPojo.filter(new FilterFunction<Exercise2.User>() {
			@Override
			public boolean filter(Exercise2.User user) throws Exception {
				return user.getCountry().equals("Hungary");
			}
		})
				.join(listens)
				.where("name")
				.equalTo("name")
				.map(new MapFunction<Tuple2<Exercise2.User,Listens>, Tuple1<String>>() {
					@Override
					public Tuple1<String> map(Tuple2<Exercise2.User, Listens> userListens) throws Exception {
						return new Tuple1<String>(userListens.f1.getSong());
					}
				})
				.distinct()
				.print();
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static CsvReader standardCsv(ExecutionEnvironment env, String path) {
		return env.readCsvFile(path)
				.ignoreFirstLine()
				.fieldDelimiter("\t")
				.lineDelimiter("\n");
	}

	public static class Listens implements Serializable {
		private String name;
		private String song;
		private String timeOfListen;

		public Listens() {
		}

		public Listens(String name, String song, String timeOfListen) {
			this.name = name;
			this.song = song;
			this.timeOfListen = timeOfListen;
		}

		public String getName() {
			return name;
		}

		public String getSong() {
			return song;
		}

		public String getTimeOfListen() {
			return timeOfListen;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setSong(String song) {
			this.song = song;
		}

		public void setTimeOfListen(String timeOfListen) {
			this.timeOfListen = timeOfListen;
		}

		@Override
		public String toString() {
			return "Listens{" +
					"name='" + name + '\'' +
					", song='" + song + '\'' +
					", timeOfListen='" + timeOfListen + '\'' +
					'}';
		}
	}

	public static class Songs {
		private String title;
		private String artist;

		public Songs(String title, String artist) {
			this.title = title;
			this.artist = artist;
		}

		public String getTitle() {
			return title;
		}

		public String getArtist() {
			return artist;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public void setArtist(String artist) {
			this.artist = artist;
		}

		@Override
		public String toString() {
			return "Songs{" +
					"title='" + title + '\'' +
					", artist='" + artist + '\'' +
					'}';
		}
	}
}