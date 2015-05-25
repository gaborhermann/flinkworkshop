package hu.sztaki.workshop.flink.day6;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import hu.sztaki.workshop.flink.day6.util.LocalExecutionUtil;

public class Exercise4 {

	/**
	 * Exercise 4.<br>
	 * <br>
	 * Create two AbstractInvokables,<br>
	 * one sending integers, and another receiving and printing them.<br>
	 * <br>
	 * Hints:<br>
	 * <br>
	 * Extend the {@link AbstractInvokable} class for a sender and a receiver.<br>
	 * Use {@link RecordWriter}s and {@link RecordReader}s respectively.<br>
	 * Use {@link Environment#getWriter(int)} and {@link Environment#getInputGate(int)} in the<br>
	 * {@link AbstractInvokable#registerInputOutput()} method.<br>
	 * <br>
	 * Implement {@link IOReadableWritable} to wrap an integer for sending.<br>
	 * <br>
	 * Use public static classes and pay attention to empty constructors.<br>
	 * <br>
	 * Wire up these two AbstractInvokables with {@link AbstractJobVertex}s.<br>
	 * Use<br>
	 * {@link AbstractJobVertex#setInvokableClass(Class)} and<br>
	 * {@link AbstractJobVertex#connectNewDataSetAsInput(AbstractJobVertex, DistributionPattern)} with<br>
	 * {@link DistributionPattern#ALL_TO_ALL}<br>
	 * <br>
	 * Add these to a {@link JobGraph} and execute it with<br>
	 * {@link LocalExecutionUtil#runOnMiniCluster(JobGraph, int, long)}.<br>
	 * <br>
	 * Don't forget to {@link RecordWriter#flush}!<br>
	 */
	public static void main(String[] args) {
		// TODO code here
	}

	// TODO create a sender and a receiver here as a public static class,
	// also create an IOReadableWriteble class here
}