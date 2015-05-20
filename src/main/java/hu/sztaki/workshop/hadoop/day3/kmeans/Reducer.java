package hu.sztaki.workshop.hadoop.day3.kmeans;

import de.jungblut.math.DoubleVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer {
    public enum Counter {
        CONVERGED
    }

    private final List<Center> centers = new ArrayList<>();

    /**
     * @todo Create the new center by summing the vectors together and dividing with their count.
     * @todo New center (DoubleVector) should be encapsulated into a Center object afterwards.
     */
    protected void reduce(Center key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
        List<VectorWritable> vectorList = new ArrayList<>();
        DoubleVector newCenter = null;
        for (VectorWritable value : values) {
            vectorList.add(new VectorWritable(value));
            if (newCenter == null)
                newCenter = value.getVector().deepCopy();
            else
                newCenter = newCenter.add(value.getVector());
        }

        newCenter = newCenter.divide(vectorList.size());
        Center center = new Center(newCenter);
        centers.add(center);
        /**
         * @todo Write out the center with the vector.
         */
        for (VectorWritable vector : vectorList) {
            context.write(center, vector);
        }

        if (center.converged(key))
            context.getCounter(Counter.CONVERGED).increment(1);
    }

    /**
     * Write out current centers.
     */
    @SuppressWarnings("deprecation")
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Configuration conf = context.getConfiguration();
        Path outPath = new Path(conf.get("centroid.path"));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outPath, true);
        try (SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), outPath,
                Center.class, IntWritable.class)) {
            final IntWritable value = new IntWritable(0);
            for (Center center : centers) {
                out.append(center, value);
            }
        }
    }
}
