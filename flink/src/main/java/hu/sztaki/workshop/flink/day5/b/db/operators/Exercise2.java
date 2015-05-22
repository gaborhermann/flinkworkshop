package hu.sztaki.workshop.flink.day5.b.db.operators;

import java.io.Serializable;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class Exercise2 {

	/**
	 * Exercise 1.<br>
	 * <br>
	 * Parse 'users.csv' as Tuples.<br>
	 * Print them.<br>
	 * <br>
	 * Parse the same data from CSV as POJO.<br>
	 * Filter the users from Mexico. Get their average age (in years).<br>
	 * <br>
	 * Get the execution plan and check it in the plan visualizer.
	 * Use these:<br>
	 * {@link ExecutionEnvironment#readCsvFile(String)}<br>
	 * {@link CsvReader#fieldDelimiter}<br>
	 * {@link CsvReader#lineDelimiter}<br>
	 * {@link CsvReader#types(Class)}<br>
	 * {@link CsvReader#pojoType(Class, String...)}<br>
	 * {@link ExecutionEnvironment#getExecutionPlan()}<br>
	 */
	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		// the first argument must be the input file
		String inputPath = args[0];

		CsvReader csvSource = env.readCsvFile(args[0])
				.fieldDelimiter("\t")
				.lineDelimiter("\n");

		// as Tuple
		DataSet<Tuple3<String, String, String>> users = csvSource
				.types(String.class, String.class, String.class);

		DataSet<User> usersPojo = csvSource
				.pojoType(User.class, new String[]{"name", "country", "dateOfBirth"});

		DataSet<Tuple1<Integer>> ages = usersPojo
				.filter(new FilterFunction<User>() {
					@Override
					public boolean filter(User user) throws Exception {
						return user.getCountry().equals("Mexico");
					}
				})
				.map(new MapFunction<User, Tuple1<Integer>>() {
					@Override
					public Tuple1<Integer> map(User user) throws Exception {
						int birthYear = Integer.parseInt(user.getDateOfBirth().substring(6, 10));
						return new Tuple1<Integer>(2015 - birthYear);
					}
				});

		ages.combineGroup(new GroupCombineFunction<Tuple1<Integer>, Double>() {
			@Override
			public void combine(Iterable<Tuple1<Integer>> iterable, Collector<Double> collector) throws Exception {
				int ageSum = 0;
				int count = 0;
				for (Tuple1<Integer> age : iterable) {
					ageSum += age.f0;
					count++;
				}
				collector.collect((double) ageSum / count);
			}
		}).print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class User implements Serializable {
		private String name;
		private String country;
		private String dateOfBirth;

		public User() {
		}

		public User(String name, String country, String dateOfBirth) {
			this.name = name;
			this.country = country;
			this.dateOfBirth = dateOfBirth;
		}

		public String getName() {
			return name;
		}

		public String getCountry() {
			return country;
		}

		public String getDateOfBirth() {
			return dateOfBirth;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public void setDateOfBirth(String dateOfBirth) {
			this.dateOfBirth = dateOfBirth;
		}

		@Override
		public String toString() {
			return "User{" +
					"name='" + name + '\'' +
					", country='" + country + '\'' +
					", dateOfBirth='" + dateOfBirth + '\'' +
					'}';
		}
	}
}