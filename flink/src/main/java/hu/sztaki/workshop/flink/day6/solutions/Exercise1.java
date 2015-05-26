package hu.sztaki.workshop.flink.day6.solutions;

import java.lang.reflect.Type;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class Exercise1 {

	/**
	 * Exercise 1.<br>
	 * <br>
	 * Create an instance of the generic class.<br>
	 * Get its type information.<br>
	 * <br>
	 * Extend the class such that the subclass does NOT have any type parameters.<br>
	 * Create an instance of it.<br>
	 * Get its type information too.<br>
	 * Get the T type parameter (e.g. String if the class extends MyType&lt;String&gt;)<br>
	 * <br>
	 * Now extend the first class such that the subclass has a type parameter.<br>
	 * Create an instance of it.<br>
	 * Try to get its type parameter.<br>
	 * <br>
	 * Hint:<br>
	 * First use {@link TypeExtractor#getForObject(Object)}.<br>
	 * Then use {@link TypeExtractor#getParameterType(Class, Class, int)}.<br>
	 * (Use {@link Object#getClass()} for the created instances.)<br>
	 */
	public static void main(String[] args) {

		// first type
		MyType<String> myObject =
				new MyType<String>(1, "wrappedVal");

		TypeInformation<MyType<String>> typeInfo = TypeExtractor.getForObject(myObject);
		System.out.println(typeInfo);

		// subclass
		MySubclass subclassInstance = new MySubclass(2, "wrappedVal");
		System.out.println(TypeExtractor.getForObject(subclassInstance));

		Type parameterType =
				TypeExtractor.getParameterType(myObject.getClass(), subclassInstance.getClass(), 0);
		System.out.println(parameterType);

		// generic subclass
		MyGenericSubclass<String> genericSubclassInstance =
				new MyGenericSubclass<String>(3, "wrappedVal");

		Type genericSubclassParameterType =
				TypeExtractor.getParameterType(myObject.getClass(), genericSubclassInstance.getClass(), 0);
		System.out.println(genericSubclassParameterType);
	}

	public static class MyType<T> {
		private int myNum;
		private T myWrappedMember;

		public MyType(int myNum, T myWrappedMember) {
			this.myNum = myNum;
			this.myWrappedMember = myWrappedMember;
		}

		public T getMyWrappedMember() {
			return myWrappedMember;
		}

		public int getMyNum() {
			return myNum;
		}

		@Override
		public String toString() {
			return "MyType{" +
					"myNum=" + myNum +
					", myWrappedMember=" + myWrappedMember +
					'}';
		}
	}

	public static class MySubclass extends MyType<String> {
		public MySubclass(int myNum, String myWrappedMember) {
			super(myNum, myWrappedMember);
		}
	}

	public static class MyGenericSubclass<T> extends MyType<T> {
		public MyGenericSubclass(int myNum, T myWrappedMember) {
			super(myNum, myWrappedMember);
		}
	}

}