package hu.sztaki.workshop.flink.day6.solutions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.util.DataOutputSerializer;

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

		// Java serialization
		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1000);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(toCopy);

			ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
			Object copiedObject = objectInputStream.readObject();

			System.out.println("size in bytes: " + byteArrayOutputStream.size());
			System.out.println("copied object: " + copiedObject);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		final int numberOfObjectsToSerialize = 100000;

		// Java serialization measure
		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(10000 * numberOfObjectsToSerialize);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

			long startTime = System.currentTimeMillis();
			for (int i = 0; i < numberOfObjectsToSerialize; i++) {
				toCopy =
						new Exercise2.ToCopyWithSerialization<String>(new ArrayList<Long>(Arrays.asList(1L, 2L, (long) i)), "Flink before you code! " + i);
				objectOutputStream.writeObject(toCopy);
			}
			System.out.println("---------------");
			System.out.println("Java serialization: ");
			System.out.println("serialization for " + numberOfObjectsToSerialize + " objects: " + (System.currentTimeMillis() - startTime) + " ms");
			System.out.println("size in bytes of " + numberOfObjectsToSerialize + " objects: " + byteArrayOutputStream.size());
			System.out.println("---------------");
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Flink serialization measure
		try {
			TypeSerializer<Exercise2.ToCopyWithSerialization<String>> serializer =
					TypeExtractor.getForObject(toCopy).createSerializer(new ExecutionConfig());

			DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(10000 * numberOfObjectsToSerialize);

			long startTime = System.currentTimeMillis();
			for (int i = 0; i < numberOfObjectsToSerialize; i++) {
				toCopy =
						new Exercise2.ToCopyWithSerialization<String>(new ArrayList<Long>(Arrays.asList(1L, 2L, (long) i)), "Flink before you code! " + i);
				serializer.serialize(toCopy, dataOutputSerializer);
			}
			System.out.println("---------------");
			System.out.println("Flink serialization: ");
			System.out.println("serialization for " + numberOfObjectsToSerialize + " objects: " + (System.currentTimeMillis() - startTime) + " ms");
			System.out.println("size in bytes of " + numberOfObjectsToSerialize + " objects: " + dataOutputSerializer.length());
			System.out.println("---------------");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}