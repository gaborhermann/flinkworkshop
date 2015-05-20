package hu.sztaki.workshop.hadoop.day2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.Iterator;

/**
 * @exercise Determine which users were mentioned in which tweet!
 * @input /workshop/mentions
 *        Structured: {NULL},{tweet_id},{mentioned_user_id},{tweet_owner}
 */
public class Mentioned extends Configured implements Tool{
    private Configuration configuration;

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new Mobile(), args));
    }

    public int run(String[] strings) throws Exception {
        createConfiguration();
        Job job = createJob();

        job.waitForCompletion(true);

        return 0;
    }

    public void createConfiguration(){
        configuration = new Configuration();
    }

    public Job createJob() throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJobName("Twitter - mentioned");

        job.setJarByClass(Mobile.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setJar("out\\artifacts\\yarn_applications_jar\\yarn_applications.jar");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/mentioned"));

        Path outputPath = new Path("/mobile-out");
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

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split(",");
            context.write(new Text(s[2]), new Text(s[1]));
        }
    }
    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder s = new StringBuilder("");

            while(values.hasNext()) {
                s.append((values.next()).toString()).append(", ");
            }

            context.write(key, new Text(s.toString()));
        }
    }
}