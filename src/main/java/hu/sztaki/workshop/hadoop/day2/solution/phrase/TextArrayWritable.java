package hu.sztaki.workshop.hadoop.day2.phrase;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class TextArrayWritable implements WritableComparable, Serializable {
    // begin of current range
    private int b = 0;

    // end of current range (exclusive)
    private int e = 0;

    private Text[] contents;

    public TextArrayWritable() {
        contents = new Text[0];
    }

    public TextArrayWritable(Text[] contents) {
        this.contents = contents;
        this.b = 0;
        this.e = contents.length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, e - b);
        for (int i = b; i < e; i++) {
            WritableUtils.writeString(out, contents[i].toString());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        contents = new Text[WritableUtils.readVInt(in)];
        for (int i = 0; i < contents.length; i++) {
            contents[i] = new Text(WritableUtils.readString(in));
        }
        b = 0;
        e = contents.length;
    }

    @Override
    public int compareTo(Object o) {
        TextArrayWritable other = (TextArrayWritable) o;
        int length = e - b;
        int otherLength = other.e - other.b;
        int minLength = (length < otherLength ? length : otherLength);
        for (int i = 0; i < minLength; i++) {
            Text tid = contents[b + i];
            Text otherTId = other.contents[other.b + i];
            if (tid.compareTo(otherTId) > 0) {
                return +1;
            } else if (tid.compareTo(otherTId) < 0) {
                return -1;
            }
        }
        return (otherLength - length);
    }

    /**
     * Returns a copy of the currently considered contents.
     *
     * @return
     */
    public Text[] getContents() {
        Text[] result = contents;
        if (b != 0 || e != contents.length) {
            result = new Text[e - b];
            System.arraycopy(contents, b, result, 0, e - b);
        }
        return result;
    }

    /**
     * Sets contents and currently considered range.
     *
     * @param contents
     * @param b
     * @param e
     */
    public void setContents(Text[] contents, int b, int e) {
        this.contents = contents;
        this.b = b;
        this.e = e;
    }

    /**
     * Sets contents.
     *
     * @param contents
     */
    public void setContents(Text[] contents) {
        this.contents = contents;
        this.b = 0;
        this.e = contents.length;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        TextArrayWritable other = (TextArrayWritable) obj;

        if ((e - b) != (other.e - other.b)) {
            return false;
        }
        for (int i = 0, len = e - b; i < len; i++) {
            if (contents[b + i] != other.contents[other.b + i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        if (contents == null) {
            return 0;
        }

        int result = 1;
        for (int i = b; i < e; i++) {
            result = 31 * result + contents[i].toString().hashCode();
        }

        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = b; i < e; i++) {
            sb.append(contents[i]);
            if (i != e - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public static final class DefaultComparator extends WritableComparator {

        public DefaultComparator() {
            super(TextArrayWritable.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextArrayWritable && b instanceof TextArrayWritable) {
                return ((TextArrayWritable) a).compareTo((TextArrayWritable) b);
            }
            return super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstLength = readVInt(b1, s1);
                int p1 = s1 + WritableUtils.decodeVIntSize(b1[s1]);
                int secondLength = readVInt(b2, s2);
                int p2 = s2 + WritableUtils.decodeVIntSize(b2[s2]);
                int minLength = (firstLength < secondLength ? firstLength : secondLength);
                for (int i = 0; i < minLength; i++) {
                    int firstTId = readVInt(b1, p1);
                    p1 += WritableUtils.decodeVIntSize(b1[p1]);
                    int secondTId = readVInt(b2, p2);
                    p2 += WritableUtils.decodeVIntSize(b2[p2]);
                    if (firstTId < secondTId) {
                        return +1;
                    } else if (firstTId > secondTId) {
                        return -1;
                    }
                }
                return (secondLength - firstLength);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    /**
     * Partitions IntArrayWritables on their content but ignores last entry.
     */
    public static final class TextArrayWritablePartitionerAllButLast extends Partitioner<TextArrayWritable, Object> {

        @Override
        public int getPartition(TextArrayWritable key, Object value, int i) {
            int result = 1;
            Text[] contents = key.getContents();
            for (int j = 0; j < contents.length - 1; j++) {
                result = result * 31 + contents[j].toString().hashCode();
            }
            result = result % i;
            return (result < 0 ? -result : result);
        }
    }

    /**
     * Partitions IntArrayWritables based on their entire content.
     */
    public static final class TextArrayWritablePartitionerComplete extends Partitioner<TextArrayWritable, Object> {

        @Override
        public int getPartition(TextArrayWritable key, Object value, int i) {
            int result = 1;
            Text[] contents = key.getContents();
            for (int j = 0; j < contents.length; j++) {
                result = result * 31 + contents[j].toString().hashCode();
            }
            result = result % i;
            return (result < 0 ? -result : result);
        }
    }

    /**
     * Partitions IntArrayWritables based on their first two entries.
     */
    public static final class TextArrayWritablePartitionerFirstTwo extends Partitioner<TextArrayWritable, Object> {

        @Override
        public int getPartition(TextArrayWritable key, Object value, int i) {
            int result = 1;
            Text[] contents = key.getContents();
            for (int j = 0; j < contents.length && j < 2; j++) {
                result = result * 31 + contents[j].toString().hashCode();
            }
            result = result % i;
            return (result < 0 ? -result : result);
        }
    }

    /**
     * Partitions IntArrayWritables based on their first entry.
     */
    public static final class TextArrayWritablePartitionerFirstOnly extends Partitioner<TextArrayWritable, Object> {

        @Override
        public int getPartition(TextArrayWritable key, Object value, int i) {
            int result = (31 * key.getContents()[0].toString().hashCode()) % i;
            return (result < 0 ? -result : result);
        }
    }
}