package hu.sztaki.workshop.hadoop.day3.kmeans;

import hu.sztaki.workshop.hadoop.day3.kmeans.distance.DistanceMeasurer;
import hu.sztaki.workshop.hadoop.day3.kmeans.distance.ManhattanDistance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @todo Implement class.
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper
        <Center, VectorWritable, Center, VectorWritable>{
    private final List<Center> centers = new ArrayList<>();
    private DistanceMeasurer distanceMeasurer;

    /**
     * @todo Setup the map function.
     * @hint You have to read the whole centers file and store the centers in
     *       an object-variable.
     */
    @SuppressWarnings("deprecation")
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path centroids = new Path(conf.get("centroid.path"));
        FileSystem fs = FileSystem.get(conf);

        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf)) {
            Center key = new Center();
            IntWritable value = new IntWritable();
            int index = 0;
            while (reader.next(key, value)) {
                Center clusterCenter = new Center(key);
                clusterCenter.setClusterIndex(index++);
                centers.add(clusterCenter);
            }
        }
        distanceMeasurer = new ManhattanDistance();
    }

    /**
     * @todo Complete map function!
     * @todo Find the closest center to vector, then write it out.
     * @hint Use a DistanceMeasurer to measure the distances from the centers.
     */
    @Override
    protected void map(Center key, VectorWritable value, Context context)
    throws IOException, InterruptedException {
        Center nearest = null;
        double nearestDistance = Double.MAX_VALUE;
        for (Center c : centers) {
            double dist = distanceMeasurer.measureDistance(c.getCenterVector(), value.getVector());
            if (nearest == null) {
                nearest = c;
                nearestDistance = dist;
            } else {
                if (nearestDistance > dist) {
                    nearest = c;
                    nearestDistance = dist;
                }
            }
        }
        context.write(nearest, value);
    }
}
