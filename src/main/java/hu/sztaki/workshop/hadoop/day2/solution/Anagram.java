package hu.sztaki.workshop.hadoop.day2.solution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @self We are going to implement this job from the ground up together!
 */
public class Anagram extends Configured implements Tool {
    private Configuration configuration;

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new Anagram(), args));
    }

    public int run(String[] strings) throws Exception {
        createConfiguration();
        Job job = createJob();

        job.waitForCompletion(true);

        return 0;
    }

    public Job createJob() throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJobName("Anagrams");

        job.setJarByClass(Anagram.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setJar("out/artifacts/yarn_applications_jar/yarn-applications.jar");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/workshop/gutenberg.txt"));

        Path outputPath = new Path("/anagram-out");
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fileSystem;
        try{
            fileSystem = FileSystem.newInstance(configuration);
            if(fileSystem.exists(outputPath))
                fileSystem.delete(outputPath, true);
        } catch (IOException e){
            e.printStackTrace();
        }

        return job;
    }

    public void createConfiguration(){
        configuration = new Configuration();
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        private Text sortedText = new Text();
        private Text orginalText = new Text();
        private String word;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word = itr.nextToken().toLowerCase();
                char[] wordChars = word.toCharArray();
                Arrays.sort(wordChars);
                String sortedWord = new String(wordChars);
                sortedText.set(sortedWord);
                orginalText.set(word);
                context.write(sortedText, orginalText);
            }
        }
    }
    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int tokens = 0;
            String output = "";
            List<String> list = new ArrayList<String>();
            for (Text value : values){
                if(!list.contains(value.toString())){
                    list.add(value.toString());
                }
            }
            for (String e : list){
                tokens ++;
                output = output + e + " ~ ";
            }
            if(tokens > 1){
                context.write(new Text(key.toString()), new Text(output));
            }
        }
    }
}
