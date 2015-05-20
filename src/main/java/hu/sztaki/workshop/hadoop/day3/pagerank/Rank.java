package hu.sztaki.workshop.hadoop.day3.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @todo Implement class.
 */
public class Rank extends Configured implements WritableComparable<Rank>{
    private Text outlink;
    private DoubleWritable rank;
    boolean isOutlink;

    public Rank(){}

    /**
     * @todo Implement.
     */
    public Rank(Rank o){
        this.outlink = o.outlink;
        this.rank = o.rank;
        this.isOutlink = o.isOutlink;
    }

    public Rank(Text outlink){
        this.outlink = outlink;
        this.rank = new DoubleWritable();
        this.isOutlink = true;
    }

    public Rank(DoubleWritable rank){
        this.rank = rank;
        this.outlink = new Text();
        this.isOutlink = false;
    }

    /**
     * @todo Implement.
     */
    @Override
    public int compareTo(Rank o) {
        if (this.isOutlink && o.isOutlink){
            return -1 * this.outlink.compareTo(o.outlink);
        }

        return -1 * this.outlink.compareTo(o.outlink);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isOutlink);
        dataOutput.writeDouble(rank.get());
        dataOutput.writeUTF(outlink.toString());
    }

    /**
     * @todo Complete method.
     * @hint Check write method for hints on how to implement it.
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isOutlink = dataInput.readBoolean();
        rank = new DoubleWritable(dataInput.readDouble());
        outlink = new Text(dataInput.readUTF());
    }

    /**
     * @todo Implement.
     */
    public String toString(){
        if (isOutlink){
            return outlink.toString();
        }
        else {
            return rank.toString();
        }
    }

    /**
     * @todo Implement.
     */
    public boolean isOutlink() {
        return isOutlink;
    }

    /**
     * @todo Implement.
     */
    public void setOutlink(boolean isOutlink) {
        this.isOutlink = isOutlink;
    }

    /**
     * @todo Implement.
     */
    public Text getOutlink() {
        return outlink;
    }

    /**
     * @todo Implement.
     */
    public void setOutlink(Text outlink) {
        this.outlink = outlink;
    }

    /**
     * @todo Implement.
     */
    public DoubleWritable getRank() {
        return rank;
    }

    /**
     * @todo Implement.
     */
    public void setRank(DoubleWritable rank) {
        this.rank = rank;
    }
}