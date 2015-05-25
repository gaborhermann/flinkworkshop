package hu.sztaki.workshop.flink.day6;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;

public class Exercise2 {

	/**
	 * Exercise 2.<br>
	 * <br>
	 * Copy an instance of the generic class with Flink serialization.<br>
	 * Serialize then deserialize.<br>
	 * <br>
	 * Hints:<br>
	 * <br>
	 * Use {@link TypeInformation#createSerializer(ExecutionConfig)}<br>
	 * Write into {@link DataOutputSerializer}<br>
	 * Read from {@link DataInputDeserializer}<br>
	 */
	public static void main(String[] args) {
		ToCopyWithSerialization<String> toCopy =
				new ToCopyWithSerialization<String>(new ArrayList<Long>(Arrays.asList(1L, 2L, 3L)), "Flink before you code!");

		// TODO code here
	}

	public static class ToCopyWithSerialization<T> implements Serializable {
		private ArrayList<Long> list;
		private T wrappedObject;

		public ToCopyWithSerialization() {
		}

		public ToCopyWithSerialization(ArrayList<Long> list, T wrappedObject) {
			this.list = list;
			this.wrappedObject = wrappedObject;
		}

		public ArrayList<Long> getList() {
			return list;
		}

		public T getWrappedObject() {
			return wrappedObject;
		}

		@Override
		public String toString() {
			return "ToCopyWithSerialization{" +
					"list=" + list +
					", wrappedObject=" + wrappedObject +
					'}';
		}
	}
}