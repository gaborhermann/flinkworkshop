package hu.sztaki.workshop.flink.day5.b.db.operators;

import static hu.sztaki.workshop.flink.day5.b.db.operators.Exercise2.*;
import static hu.sztaki.workshop.flink.day5.b.db.operators.Exercise3.standardCsv;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import hu.sztaki.workshop.flink.day5.b.db.operators.Exercise3.Listens;

public class Exercise4 {

	/**
	 * Exercise 3.<br>
	 * <br>
	 * Determine the top 5 most listened artist by users from Mexico.<br>
	 * <br>
	 * You have just found out that the surname can uniquely identify a user.<br>
	 * Use KeySelectors to only match by surname and optimize the size of the keys.<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(3);

		CsvReader listensCsv = standardCsv(env, args[0]);
		DataSource<Listens> listens = listensCsv
				.pojoType(Listens.class, new String[]{"name", "song", "timeOfListen"});

		CsvReader songsCsv = standardCsv(env, args[1]);
		DataSource<Song> songs = songsCsv
				.pojoType(Song.class, new String[]{"title", "artist"});

		CsvReader usersCsv = standardCsv(env, args[2]);
		DataSet<User> usersPojo = usersCsv
				.pojoType(User.class, new String[]{"name", "country", "dateOfBirth"});

		// filter users from Mexico
		DataSet<User> fromMexico = usersPojo.filter(new FilterFunction<User>() {
			@Override
			public boolean filter(User user) throws Exception {
				return user.getCountry().equals("Mexico");
			}
		});

		DataSet<Listens> fromMexicoListens = fromMexico
				.join(listens)
				.where("name")
				.equalTo("name")
//				optimization with KeySelector
//				.where(new KeySelector<User, String>() {
//					@Override
//					public String getKey(User user) throws Exception {
//						return user.getName().split(" ")[1];
//					}
//				})
//				.equalTo(new KeySelector<Listens, String>() {
//					@Override
//					public String getKey(Listens listen) throws Exception {
//						return listen.getName().split(" ")[1];
//					}
//				})
				.map(new MapFunction<Tuple2<User, Listens>, Listens>() {
					@Override
					public Listens map(Tuple2<User, Listens> userListen) throws Exception {
						return userListen.f1;
					}
				});

		fromMexicoListens
				.join(songs)
				.where("song")
				.equalTo("title")
//				 mapping to artist and 1 count
				.map(new MapFunction<Tuple2<Listens, Song>, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> map(Tuple2<Listens, Song> listenAndSong) throws Exception {
						return new Tuple2<String, Integer>(listenAndSong.f1.getArtist(), 1);
					}
				})
				// group by artist
				.groupBy(0)
				// sum the listens
				.sum(1)
				// sort by the sum of listens0
				.sortPartition(1, Order.DESCENDING)
				// making sure not to individually sort all partitions
				.setParallelism(1)
				// get the first five
				.first(5)
				.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class Song {

		private String title;
		private String artist;

		public Song() {
		}

		public Song(String title, String artist) {
			this.title = title;
			this.artist = artist;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public String getArtist() {
			return artist;
		}

		public void setArtist(String artist) {
			this.artist = artist;
		}

		@Override
		public String toString() {
			return "Song{" +
					"title='" + title + '\'' +
					", artist='" + artist + '\'' +
					'}';
		}
	}
}