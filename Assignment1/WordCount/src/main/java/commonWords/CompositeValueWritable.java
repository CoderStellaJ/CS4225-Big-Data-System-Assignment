package commonWords;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeValueWritable implements WritableComparable<CompositeValueWritable> {
    private int frequency;
    private int mapId;

    public CompositeValueWritable() { }

    public CompositeValueWritable(int frequency, int mapId) {
        //mapId can either be 1 or 2 for mapper 1 and mapper 2
        //frequency should be a positive int
        this.set(frequency, mapId);
    }

    public void set(int frequency, int mapId) {
        this.frequency = frequency;
        this.mapId = mapId;
    }

    public int getId() {
        return this.mapId;
    }

    public int getFrequency() {
        return this.frequency;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(frequency);
        dataOutput.writeInt(mapId);
    }

    public void readFields(DataInput dataInput) throws IOException {
        frequency = dataInput.readInt();
        mapId = dataInput.readInt();
    }

    public boolean equals(Object o) {
        if (!(o instanceof CompositeValueWritable))
            return false;
        CompositeValueWritable other = (CompositeValueWritable) o;
        return (this.frequency == other.frequency) && (this.mapId == other.mapId);
    }

    public int compareTo(CompositeValueWritable o) {
        int thisFrequency = this.frequency;
        int thatFrequency = o.frequency;
        return (thisFrequency<thatFrequency ? -1 : (thisFrequency==thatFrequency ? 0 : 1));
    }

}

