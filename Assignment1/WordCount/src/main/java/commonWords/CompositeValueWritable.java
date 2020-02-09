package commonWords;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeValueWritable implements Writable {
    private IntWritable mapId;
    private IntWritable frequency;

    public CompositeValueWritable() {
        //mapId can either be 1 or 2 for mapper 1 and mapper 2
        //frequency should be a positive int
        mapId = new IntWritable(1);
        frequency = new IntWritable(1);
    }

    public CompositeValueWritable(int mapId, int frequency) {
        this.mapId = new IntWritable(mapId);
        this.frequency = new IntWritable(frequency);
    }

    public IntWritable getId() {
        return this.mapId;
    }

    public IntWritable getFrequency() {
        return this.frequency;
    }

    public void write(DataOutput dataOutput) throws IOException {
        mapId.write(dataOutput);
        frequency.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        mapId.readFields(dataInput);
        frequency.readFields(dataInput);
    }

}

