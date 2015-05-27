package hu.sztaki.workshop.flink.utils.data;

import static hu.sztaki.workshop.flink.utils.Generator.randomInRange;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class RadioSource implements SourceFunction<String> {

	private volatile boolean isRunning = false;
	private int minWaitTime;
	private int maxWaitTime;

	public RadioSource(int minWaitTime, int maxWaitTime) {
		this.minWaitTime = minWaitTime;
		this.maxWaitTime = maxWaitTime;
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
			songIdx = randomInRange(random, 0, 9);
			String song = StaticData.SONGS.get(songIdx).f0;

			DateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy. HH:mm:ss");
			Date date = new Date();
			String dateStr = dateFormat.format(date);
			collector.collect(song + "\t" + dateStr);

			Thread.sleep(randomInRange(random, minWaitTime, maxWaitTime));
		}
	}

	public void cancel() {
		isRunning = false;
	}
}
