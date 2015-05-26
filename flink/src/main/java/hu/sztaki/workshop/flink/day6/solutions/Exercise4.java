package hu.sztaki.workshop.flink.day6.solutions;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
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
		AbstractJobVertex sender = new AbstractJobVertex("sender");
		sender.setInvokableClass(MySender.class);
		sender.setParallelism(1);

		AbstractJobVertex receiver = new AbstractJobVertex("receiver");
		receiver.setInvokableClass(MyReciever.class);
		receiver.setParallelism(1);

		receiver.connectNewDataSetAsInput(sender, DistributionPattern.ALL_TO_ALL);

		JobGraph jobGraph = new JobGraph("my first job");
		jobGraph.addVertex(sender);
		jobGraph.addVertex(receiver);

		try {
			LocalExecutionUtil.runOnMiniCluster(jobGraph, 3, 64L);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MySender extends AbstractInvokable {

		private RecordWriter<IntIOReadableWritable> recordWriter;

		public MySender() {
		}

		@Override
		public void registerInputOutput() {
			Environment environment = getEnvironment();
			ResultPartitionWriter writer = environment.getWriter(0);
			recordWriter = new RecordWriter<IntIOReadableWritable>(writer);
		}

		@Override
		public void invoke() throws Exception {
			for (int i = 0; i < 10; i++) {
				recordWriter.emit(new IntIOReadableWritable(i));
			}
			recordWriter.flush();
		}
	}

	public static class MyReciever extends AbstractInvokable {

		private RecordReader<IntIOReadableWritable> recordReader;

		public MyReciever() {
		}

		@Override
		public void registerInputOutput() {
			Environment environment = getEnvironment();
			InputGate inputGate = environment.getInputGate(0);
			recordReader = new RecordReader<IntIOReadableWritable>(inputGate, IntIOReadableWritable.class);
		}

		@Override
		public void invoke() throws Exception {
			while (recordReader.hasNext()) {
				IntIOReadableWritable next = recordReader.next();
				System.out.println(next.getNumber());
			}
		}
	}

	public static class IntIOReadableWritable implements IOReadableWritable {

		private int number;

		public IntIOReadableWritable() {
		}

		public IntIOReadableWritable(int number) {
			this.number = number;
		}

		public int getNumber() {
			return number;
		}

		public void write(DataOutputView dataOutputView) throws IOException {
			dataOutputView.writeInt(number);
		}

		public void read(DataInputView dataInputView) throws IOException {
			number = dataInputView.readInt();
		}
	}
}