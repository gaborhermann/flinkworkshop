package hu.sztaki.workshop.flink.day6;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;

public class Exercise3 {

	/**
	 * Exercise 3.<br>
	 * <br>
	 * Serialize the class in the previous exercise with Java serialization.<br>
	 * Compare Flink and Java serialization by time and size. (You can copy your code from the previous exercise.)<br>
	 * Execute serialization multiple times in a for loop to measure time.<br>
	 * Create different objects for the measurement.<br>
	 * <br>
	 * Hints:<br>
	 * Use {@link ByteArrayOutputStream} and {@link ObjectOutputStream} for<br>
	 * serialization and {@link ByteArrayOutputStream} and {@link java.io.ObjectInputStream} for deserialization.<br>
	 */
	public static void main(String[] args) {
		Exercise2.ToCopyWithSerialization<String> toCopy =
				new Exercise2.ToCopyWithSerialization<String>(new ArrayList<Long>(Arrays.asList(1L, 2L, 3L)), "Flink before you code!");

		// TODO code here
	}
}