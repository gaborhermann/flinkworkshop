package hu.sztaki.workshop.flink.day6;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

public class Exercise5 {

	/**
	 * Exercise 5.<br>
	 * <br>
	 * Implement WordCount using Flink runtime layer as in the previous exercise.<br>
	 * <br>
	 * Use a custom {@link IOReadableWritable} for storing a word.<br>
	 * Use a {@link ChannelSelector} and select channels by the hash code of the word.<br>
	 * <br>
	 * Read from file input (you can use {@link java.io.BufferedReader}). The path should be<br>
	 * given as the first program argument. Pass this path to your file reader AbstractInvokable via<br>
	 * {@link Configuration}. Use {@link AbstractJobVertex#getConfiguration()} and<br>
	 * {@link Configuration#setString(String, String)} then get the config with<br>
	 * {@link AbstractInvokable#getTaskConfiguration()} and {@link Configuration#getString(String, String)}<br>
	 * <br>
	 * Try it on hamlet.txt<br>
	 * The word "be" occurs 190 times in the text.<br>
	 * <br>
	 * Experiment with different parallelisms. Use {@link AbstractJobVertex#setParallelism(int)}<br>
	 */
	public static void main(String[] args) {
		// TODO code here
	}

	// TODO create a file reader and a counter as AbstractInvokables
	// TODO create an IOReadableWritable for storing a word and a channel selector
	// You can copy your code from the previous exercise
}