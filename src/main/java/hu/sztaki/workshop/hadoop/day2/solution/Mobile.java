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

/**
 * @exercise Determine how many tweets are received from mobile devices!
 *           If a tweet has been sent from mobile device, it's <i>lon</i>
 *           and <i>lat</i> fields are not empty.
 *
 * @input /workshop/tweet
 */
public class Mobile extends Configured implements Tool{
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
        job.setJobName("Twitter - mobile");

        job.setJarByClass(Mobile.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setJar("out/artifacts/yarn_applications_jar/yarn-applications.jar");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/workshop/tweets"));

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

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split(",");
            if(!"".equals(s[6]) && !"".equals(s[7])){
                context.write(new Text(s[1]), new IntWritable(1));
            }
        }
    }
    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}