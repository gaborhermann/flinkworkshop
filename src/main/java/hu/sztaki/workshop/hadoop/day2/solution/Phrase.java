package hu.sztaki.workshop.hadoop.day2.solution;

import hu.sztaki.workshop.hadoop.day2.phrase.TextArrayWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @exercise We are going to implement the naive version of n-grams, which
 *           is a statistics about variable-length word sequences.
 *           Many applications use this including information retrieval,
 *           natural language processing and digital humanities.
 *
 *           We are searching for n-grams that occur tau times and consists of at
 *           most sigma words. Can be seen as a special case of frequent sequence mining.
 *           This is a generalization of word count.
 *
 * @input /workshop/gutenberg.txt
 */
public class Phrase extends Configured implements Tool {
    /**
     * @todo Complete class.
     */
    public static class Map extends Mapper<LongWritable, Text, TextArrayWritable, IntWritable> {
        // Minimum support threshold
        private int minimum_support;

        // Maximum n-gram length considered
        private int maximum_length;

        // Singleton output key -- for efficiency reasons
        private final TextArrayWritable outKey = new TextArrayWritable();

        // Singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable();

        /**
         * @todo Complete setup.
         * @hint Load configurations.
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minimum_support = context.getConfiguration().getInt("hu.sztaki.workshop.hadoop.phrase.minimum_support", 1);
            maximum_length = context.getConfiguration().getInt("hu.sztaki.workshop.hadoop.phrase.maximum_length", Integer.MAX_VALUE);
        }

        /**
         * @todo Complete map function.
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String values[] = value.toString().split(" ");
            Text contents[] = new Text[values.length];
            for(int j = 0; j < values.length; j++){
                contents[j] = new Text(values[j]);
            }
            /**
             * @hint When you write out an n-gram with value 1, instantiate a TextArrayWritable and
             *       just put the whole <i>contents</i> array into it with <i>setContents</i>,
             *       but set a range that specifies the n-gram. For example:
             *       <i>outKey.setContents(contents, start, end);</i>
             *       TextArrayWritable will handle the rest.
             * @see TextArrayWritable
             */
            for (int a = 0, b = 0; a < contents.length; a++) {
                // Determine segment end
                b = (maximum_length == Integer.MAX_VALUE) ? maximum_length : a + maximum_length;
                if(b > contents.length)
                    b = contents.length;


                // Extract all subsequences
                for (int i = a; i <= b; i++) {
                    outKey.setContents(contents, a, i);
                    outValue.set(1);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class Combine extends Reducer<TextArrayWritable, IntWritable, TextArrayWritable, IntWritable> {
        private final IntWritable outValue = new IntWritable();

        /**
         * @todo Complete function!
         */
        @Override
        protected void reduce(TextArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int support = 0;
            for (IntWritable value : values) {
                support += value.get();
            }
            outValue.set(support);
            context.write(key, outValue);
        }
    }

    public static class Reduce extends Reducer<TextArrayWritable, IntWritable, Text, Text> {
        // minimum support threshold
        private int minimum_support;

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable();

        /**
         * @todo Complete function!
         * @hint You need the minimum support parameter from configuration.
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minimum_support = context.getConfiguration().getInt("hu.sztaki.workshop.hadoop.phrase.minimum_support", 1);
        }

        /**
         * @todo Complete function!
         */
        @Override
        protected void reduce(TextArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int support = 0;
            for (IntWritable value : values) {
                support += value.get();
            }
            if (support >= minimum_support) {
                // skip if maximal n-grams desired
                outValue.set(support);

                StringBuilder builder = new StringBuilder();

                for (Text text : key.getContents()) {
                    if (builder.length() > 0) {
                        builder.append(" ");
                    }
                    builder.append(text.toString());
                }

                context.write(new Text(builder.toString()), new Text(outValue.toString()));
            }
        }
    }

    public int run(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        if (args.length < 6) {
            System.err.println("Please specify <input> <output> <minimum_support> <maximum_length> <number_of_reducers> as parameters");
        }

        // Reading job parameters from commandline arguments
        String input_path = args[0];
        String output_path = args[1];
        int minimum_support = Integer.parseInt(args[2]);
        int maximum_length = Integer.parseInt(args[3]);
        maximum_length = (maximum_length == 0 ? Integer.MAX_VALUE : maximum_length);

        int numred = Integer.parseInt(args[4]);

        /**
         * @todo Delete output directory if exists.
         */
        FileSystem.get(getConf()).delete(new Path(args[1]), true);

        /**
         * @todo Create job and configure it.
         * @todo Also set minimum support and maximum length parameters to configuration.
         * @todo Don't forget to set the number of reducers.
         * @todo Set a partitioner and sort comparator class. (Use TextArrayWritable.)
         * @remember We are reading from a sequence file, so use the appropriate file formats.
         */
        Job job = Job.getInstance(getConf());

        job.setJobName("N-grams naive (" + minimum_support + ", " + maximum_length + ")");
        job.setJarByClass(Phrase.class);
        job.setJar("out/artifacts/yarn_applications_jar/yarn-applications.jar");
        job.getConfiguration().setInt("hu.sztaki.workshop.hadoop.phrase.minimum_support", minimum_support);
        job.getConfiguration().setInt("hu.sztaki.workshop.hadoop.phrase.maximum_length", maximum_length);

        SequenceFileInputFormat.setInputPaths(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Phrase.Map.class);
        job.setReducerClass(Phrase.Reduce.class);

        job.setNumReduceTasks(numred);

        job.setMapOutputKeyClass(TextArrayWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setCombinerClass(Combine.class);
        job.setPartitionerClass(TextArrayWritable.TextArrayWritablePartitionerFirstOnly.class);
        job.setSortComparatorClass(TextArrayWritable.DefaultComparator.class);

        /**
         * @todo Start job.
         */
        job.waitForCompletion(true);

        /**
         * @todo Print run time.
         */
        System.err.println("Took " + (System.currentTimeMillis() - start) + " ms");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Phrase(), args));
    }
}
