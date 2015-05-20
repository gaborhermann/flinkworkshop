package hu.sztaki.workshop.hadoop.day3;

import hu.sztaki.workshop.hadoop.day3.kmeans.Center;
import hu.sztaki.workshop.hadoop.day3.kmeans.Mapper;
import hu.sztaki.workshop.hadoop.day3.kmeans.Reducer;
import hu.sztaki.workshop.hadoop.day3.kmeans.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @exercise Implement k-means clustering algorithm.
 *           First, pick <i>k</i> cluster centers randomly,
 * @input No input file. We generate them.
 */
public class Means {
    private static final Logger LOG = LogManager.getLogger(Means.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        /**
         * @todo Create a configuration and set number of iterations.
         */
        int iteration = 1;

        Configuration conf = new Configuration();
        conf.setInt("num.iteration", iteration);

        /**
         * @todo Create input, output and center path.
         */
        Path input_path     = new Path(args[0] + iteration);
        Path center_path    = new Path(args[1] + iteration);
        Path output_path    = new Path(args[2] + iteration);

        conf.setStrings("centroid.path", center_path.toString());

        Job job = Job.getInstance(conf);
        job.setJobName("k-means clustering initial iteration");

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setJarByClass(Means.class);
        job.setJar("out/artifacts/yarn_applications_jar/yarn-applications.jar");

        FileInputFormat.addInputPath(job, input_path);
        FileSystem fileSystem = FileSystem.newInstance(conf);

        if(fileSystem.exists(input_path)){
            fileSystem.delete(input_path, true);
        }
        if(fileSystem.exists(center_path)){
            fileSystem.delete(center_path, true);
        }
        if(fileSystem.exists(output_path)){
            fileSystem.delete(output_path, true);
        }

        /**
         * @todo Use writeExampleCenters and writeExampleVectors methods to create input and center files.
         */
        writeExampleCenters(conf, center_path, fileSystem);
        writeExampleVectors(conf, input_path, fileSystem);

        FileOutputFormat.setOutputPath(job, output_path);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Center.class);
        job.setOutputValueClass(VectorWritable.class);

        job.waitForCompletion(true);

        long counter = job.getCounters().findCounter(Reducer.Counter.CONVERGED).getValue();

        iteration++;

        while(counter > 0){
            conf = new Configuration();
            conf.setStrings("controid.path", center_path.toString());
            conf.setInt("num.iteration", iteration);
            job = Job.getInstance(conf);
            job.setJobName("k-means clustering iteration " + iteration);

            job.setMapperClass(Mapper.class);
            job.setReducerClass(Reducer.class);
            job.setJarByClass(Means.class);

            input_path  = new Path(args[0] + (iteration - 1) + "/");
            output_path = new Path(args[2] + iteration);

            FileInputFormat.addInputPath(job, input_path);
            FileOutputFormat.setOutputPath(job, output_path);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setOutputKeyClass(Center.class);
            job.setOutputValueClass(VectorWritable.class);

            job.waitForCompletion(true);

            iteration++;

            counter = job.getCounters().findCounter(Reducer.Counter.CONVERGED).getValue();
        }

        Path result = new Path(args[2] + (iteration - 1) + "/");

        FileStatus[] fIleStatuses = fileSystem.listStatus(result);

        for(FileStatus status : fIleStatuses){
            if(!status.isDirectory()){
                Path path = status.getPath();
                if(!path.getName().equals("_SUCCESS")){
                    LOG.info("Found " + path.toString());
                    try(SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, conf)){
                        Center key = new Center();
                        VectorWritable v = new VectorWritable();
                        while(reader.next(key, v)){
                            LOG.info(key + " / " + v);
                        }
                    }
                }
            }
        }
    }

    public static void writeExampleVectors(Configuration conf, Path in, FileSystem fs) throws IOException {
        try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, in, Center.class,
                VectorWritable.class)) {
            dataWriter.append(new Center(new VectorWritable(0, 0)), new VectorWritable(1, 2));
            dataWriter.append(new Center(new VectorWritable(0, 0)), new VectorWritable(16, 3));
            dataWriter.append(new Center(new VectorWritable(0, 0)), new VectorWritable(3, 3));
            dataWriter.append(new Center(new VectorWritable(0, 0)), new VectorWritable(2, 2));
            dataWriter.append(new Center(new VectorWritable(0, 0)), new VectorWritable(2, 3));
            dataWriter.append(new Center(new VectorWritable(0, 0)), new VectorWritable(25, 1));
            dataWriter.append(new Center(new VectorWritable(0, 0)), new VectorWritable(7, 6));
            dataWriter.append(new Center(new VectorWritable(0, 0)), new VectorWritable(6, 5));
            dataWriter.append(new Center(new VectorWritable(0, 0)), new VectorWritable(-1, -23));
        }
    }

    @SuppressWarnings("deprecation")
    public static void writeExampleCenters(Configuration conf, Path center, FileSystem fs) throws IOException {
        try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Center.class,
                IntWritable.class)) {
            final IntWritable value = new IntWritable(0);
            centerWriter.append(new Center(new VectorWritable(1, 1)), value);
            centerWriter.append(new Center(new VectorWritable(5, 5)), value);
        }
    }
}


