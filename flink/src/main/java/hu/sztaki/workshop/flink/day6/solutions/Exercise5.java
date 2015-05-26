package hu.sztaki.workshop.flink.day6.solutions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import hu.sztaki.workshop.flink.day6.util.LocalExecutionUtil;

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
		AbstractJobVertex fileReader = new AbstractJobVertex("file reader");
		fileReader.setInvokableClass(MyFileReader.class);
		fileReader.setParallelism(1);

		Configuration fileReaderConfiguration = fileReader.getConfiguration();
		fileReaderConfiguration.setString("file path", args[0]);

		AbstractJobVertex counter = new AbstractJobVertex("counter");
		counter.setInvokableClass(MyCounter.class);
		counter.setParallelism(3);

		counter.connectNewDataSetAsInput(fileReader, DistributionPattern.ALL_TO_ALL);

		JobGraph jobGraph = new JobGraph("word count");
		jobGraph.addVertex(fileReader);
		jobGraph.addVertex(counter);

		try {
			LocalExecutionUtil.runOnMiniCluster(jobGraph, 4, 64L);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyFileReader extends AbstractInvokable {

		private RecordWriter<Word> recordWriter;
		private String filePath;

		public MyFileReader() {
		}

		@Override
		public void registerInputOutput() {
			Configuration taskConfiguration = getTaskConfiguration();
			filePath = taskConfiguration.getString("file path", null);

			Environment environment = getEnvironment();
			ResultPartitionWriter writer = environment.getWriter(0);
			recordWriter = new RecordWriter<Word>(writer, new HashChannelSelector());
		}

		@Override
		public void invoke() throws Exception {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(filePath)));

			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				String[] words = line.split("\\W+");
				for (String word : words) {
					recordWriter.emit(new Word(word));
				}
			}

			recordWriter.flush();
		}
	}

	public static class MyCounter extends AbstractInvokable {

		private RecordReader<Word> recordReader;
		private HashMap<String, Integer> wordCount;
		private int indexOfSubtask;

		public MyCounter() {
		}

		@Override
		public void registerInputOutput() {
			Environment environment = getEnvironment();
			InputGate inputGate = environment.getInputGate(0);
			recordReader = new RecordReader<Word>(inputGate, Word.class);
			indexOfSubtask = getIndexInSubtaskGroup();
		}

		@Override
		public void invoke() throws Exception {
			wordCount = new HashMap<String, Integer>();

			while (recordReader.hasNext()) {
				Word next = recordReader.next();
				Integer count = wordCount.get(next.getWord());

				if (count == null) {
					count = 0;
				}

				wordCount.put(next.getWord(), count + 1);
			}

			for (Map.Entry<String, Integer> wordCountEntry : wordCount.entrySet()) {
				System.out.println(indexOfSubtask + "> " + wordCountEntry);
			}
		}
	}

	public static class HashChannelSelector implements ChannelSelector<Word> {

		private int[] outputChannel;

		public HashChannelSelector() {
			this.outputChannel = new int[]{-1};
		}

		public int[] selectChannels(Word word, int numberOfChannels) {
			outputChannel[0] = Math.abs(word.getWord().hashCode() % numberOfChannels);
			return outputChannel;
		}
	}

	public static class Word implements IOReadableWritable {

		private String word;

		public Word() {
		}

		public Word(String word) {
			this.word = word;
		}

		public String getWord() {
			return word;
		}

		public void write(DataOutputView dataOutputView) throws IOException {
			dataOutputView.writeUTF(word);
		}

		public void read(DataInputView dataInputView) throws IOException {
			word = dataInputView.readUTF();
		}
	}
}