/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hu.sztaki.workshop.flink.utils;

import java.util.List;
import java.util.Random;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Generator {

	private static final int NUMBER_OF_LISTENS = 537;

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
		Random random = new Random();
		DataSet<String> songs = env.readCsvFile("/home/ghermann/git/gaborhermann/flinkworkshop/flink/src/main/resources/songs.csv")
				.fieldDelimiter("\t")
				.lineDelimiter("\n")
				.types(String.class, String.class)
				.map(x -> "new Tuple2<String, String>(\"" + x.f0 + "\", \"" + x.f1 + "\"),").returns(String.class);


		DataSet<String> users = env.readCsvFile("/home/ghermann/git/gaborhermann/flinkworkshop/flink/src/main/resources/users.csv")
				.fieldDelimiter("\t")
				.lineDelimiter("\n")
				.types(String.class, String.class, String.class)
				.map(x -> "new Tuple3<String, String, String>(\"" + x.f0 + "\", \"" + x.f1 + "\", \"" + x.f2 + "\"),").returns(String.class);
//		users.print();
		songs.print();
		List<String> songList = songs.collect();
		List<String> userList = users.collect();


		songList.stream().forEach(System.out::print);
		userList.stream().forEach(System.out::print);

		for (int i = 0; i < NUMBER_OF_LISTENS; i++) {
			// date
			int day = randomInRange(random, 1, 15);
			int hour;
			if (random.nextDouble() < 0.87) {
				hour = randomInRange(random, 7, 23);
			} else {
				hour = randomInRange(random, 0, 6);
			}
			int minute = randomInRange(random, 0, 59);
			String dayStr = String.format("%02d", day);
			String hourStr = String.format("%02d", hour);
			String minStr = String.format("%02d", minute);

			int songIdx;
			if (random.nextDouble() < 0.34) {
				songIdx = randomInRange(random, 0, 9);
			} else if (random.nextDouble() < 0.71) {
				songIdx = randomInRange(random, 10, 50);
			} else {
				songIdx = randomInRange(random, 51, 99);
			}
			String song = songList.get(songIdx);

			int userIdx;
			if (random.nextDouble() < 0.21) {
				userIdx = randomInRange(random, 0, userList.size() / 2);
			} else {
				userIdx = randomInRange(random, userList.size() / 2, userList.size() - 1);
			}

			String date = dayStr + ".05.2015. " + hourStr + ":" + minStr;
			String user = userList.get(userIdx);
//			System.out.println(user + "\t" + song + "\t" + date);
		}
	}

	public static String randomDate(Random random) {
		return String.format("%02d", randomInRange(random, 1, 28)) + "." +
				String.format("%02d", randomInRange(random, 1, 12)) + "." +
				randomInRange(random, 1940, 2008) + ".";
	}

	public static int randomInRange(Random rand, int lowest, int highest) {
		return lowest + rand.nextInt(highest - lowest + 1);
	}
}
