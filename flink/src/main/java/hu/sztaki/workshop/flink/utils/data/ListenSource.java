package hu.sztaki.workshop.flink.utils.data;

import static hu.sztaki.workshop.flink.utils.Generator.randomInRange;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class ListenSource implements SourceFunction<String> {

	private volatile boolean isRunning = false;
	private int minWaitTime;
	private int maxWaitTime;
	private int numOfListensInSpike;
	private int maxWaitTimeInSpike;

	public ListenSource(int minWaitTime, int maxWaitTime) {
		this.minWaitTime = minWaitTime;
		this.maxWaitTime = maxWaitTime;
		this.numOfListensInSpike = 0;
	}

	public void run(Collector<String> collector) throws Exception {
		isRunning = true;

		Random random = new Random();
		while (isRunning) {
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
			String song = StaticData.SONGS.get(songIdx).f0;

			int userIdx;
			if (random.nextDouble() < 0.21) {
				userIdx = randomInRange(random, 0, StaticData.USERS.size() / 2);
			} else {
				userIdx = randomInRange(random, StaticData.USERS.size() / 2, StaticData.USERS.size() - 1);
			}

			DateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy. HH:mm:ss");
			Date date = new Date();
			String dateStr = dateFormat.format(date);
			String user = StaticData.USERS.get(userIdx).f0;
			collector.collect(user + "\t" + song + "\t" + dateStr);

			long sleepTime;
			if (numOfListensInSpike == 0) {
				sleepTime = randomInRange(random, minWaitTime, maxWaitTime);
				if (random.nextDouble() < 0.1) {
					numOfListensInSpike = randomInRange(random, 300, 400);
				}
			} else {
				numOfListensInSpike--;
				sleepTime = randomInRange(random, 1, minWaitTime / 4 + 1);
			}

			Thread.sleep(sleepTime);
		}
	}

	public void cancel() {
		isRunning = false;
	}
}
