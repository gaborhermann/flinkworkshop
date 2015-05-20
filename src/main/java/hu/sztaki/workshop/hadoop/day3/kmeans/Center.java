package hu.sztaki.workshop.hadoop.day3.kmeans;

import de.jungblut.math.DoubleVector;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class Center implements WritableComparable<Center>{
    private DoubleVector center;
    private int times_incremented = 1;
    private int cluster_index;

    public Center(){
        super();
    }

    public Center(DoubleVector center){
        super();
        this.center = center.deepCopy();
    }

    public Center(Center center){
        super();
        this.center = center.center.deepCopy();
        this.times_incremented = center.times_incremented;
    }

    public Center(VectorWritable center){
        super();
        this.center = center.getVector().deepCopy();
    }

    public final void plus(VectorWritable c){
        plus(c.getVector());
    }

    public final void plus(DoubleVector c){
        center = center.add(c);
        times_incremented++;
    }

    public final void plus(Center c) {
        times_incremented += c.times_incremented;
        center = center.add(c.getCenterVector());
    }

    public final void divideByTimesIncremented(){
        center = center.divide(times_incremented);
    }

    public final boolean converged(Center c){
        return calculateError(c.getCenterVector()) > 0;
    }

    public final double calculateError(DoubleVector v){
        return Math.sqrt(center.subtract(v).abs().sum());
    }

    public final DoubleVector getCenterVector(){
        return center;
    }

    public int getClusterIndex(){
        return cluster_index;
    }

    public void setClusterIndex(int cluster_index){
        this.cluster_index = cluster_index;
    }
    @Override
    public final boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Center other = (Center) obj;
        if (center == null) {
            if (other.center != null)
                return false;
        } else if (!center.equals(other.center))
            return false;
        return true;
    }

    @Override
    public final String toString() {
        return "ClusterCenter [center=" + center + "]";
    }

    @Override
    public final int hashCode(){
        final int prime = 31;
        int result = 1;
        result = prime * result + ((center == null) ? 0 : center.hashCode());
        return result;
    }

    @Override
    public final void write(DataOutput dataOutput) throws IOException{
        VectorWritable.writeVector(center, dataOutput);
        dataOutput.writeInt(times_incremented);
        dataOutput.writeInt(cluster_index);
    }

    @Override
    public int compareTo(Center o) {
        return Integer.compare(cluster_index, o.cluster_index);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.center = VectorWritable.readVector(dataInput);
        times_incremented = dataInput.readInt();
        cluster_index = dataInput.readInt();
    }
}