package hu.sztaki.workshop.flink.day6;

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
	 * First use {@link org.apache.flink.api.java.typeutils.TypeExtractor#getForObject(Object)}.<br>
	 * Then use {@link org.apache.flink.api.java.typeutils.TypeExtractor#getParameterType(Class, Class, int)}.<br>
	 * (Use {@link Object#getClass()} for the created instances.)<br>
	 */
	public static void main(String[] args) {

		MyType<String> myObject =
				new MyType<String>(1, "wrappedVal");

		// TODO code here
	}

	// TODO create the mentioned classes as public static classes

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

}