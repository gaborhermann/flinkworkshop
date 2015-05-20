package hu.sztaki.workshop.hadoop.day3.kmeans;

import de.jungblut.math.DoubleVector;
import de.jungblut.math.dense.DenseDoubleVector;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @todo Implement this.
 */
public final class VectorWritable implements WritableComparable<VectorWritable>{
    private DoubleVector vector;

    public VectorWritable(){
        super();
    }

    public VectorWritable(VectorWritable v){
        this.vector = v.getVector();
    }

    public VectorWritable(DenseDoubleVector v){
        this.vector = v;
    }

    public VectorWritable(double x){
        this.vector = new DenseDoubleVector(new double[] { x });
    }

    public VectorWritable(double x, double y) {
        this.vector = new DenseDoubleVector(new double[] { x, y });
    }

    public VectorWritable(double[] array){
        this.vector = new DenseDoubleVector(array);
    }

    @Override
    public int compareTo(VectorWritable o) {
        return compareVector(this, o);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        writeVector(this.vector, dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        this.vector = readVector(dataInput);
    }

    @Override
    public int hashCode(){
        final int prime = 31;
        int result = 1;
        result = prime * result + ((vector == null) ? 0 : vector.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj) return true;
        if(obj == null) return false;
        if(getClass() != obj.getClass()) return false;
        VectorWritable other = (VectorWritable) obj;
        if(vector == null){
            if(other.vector != null) return false;
        }else if(!vector.equals(other.vector)){
            return false;
        }
        return true;
    }

    public DoubleVector getVector(){
        return vector;
    }

    @Override
    public String toString(){
        return vector.toString();
    }

    public static void writeVector(DoubleVector vector, DataOutput dataOutput) throws IOException{
        dataOutput.writeInt(vector.getLength());
        for(int i = 0; i < vector.getDimension(); i++){
            dataOutput.writeDouble(vector.get(i));
        }
    }

    public static DoubleVector readVector(DataInput dataInput) throws IOException{
        final int length = dataInput.readInt();
        DoubleVector vector = new DenseDoubleVector(length);
        for(int i = 0; i < length; i++){
            vector.set(i, dataInput.readDouble());
        }
        return vector;
    }

    public static int compareVector(VectorWritable a, VectorWritable o){
        return compareVector(a.getVector(), o.getVector());
    }

    public static int compareVector(DoubleVector a, DoubleVector o){
        DoubleVector subtract = a.subtract(o);
        return (int) subtract.sum();
    }

    public static VectorWritable wrap(DenseDoubleVector a){
        return new VectorWritable(a);
    }
}