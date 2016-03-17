import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by beyondwu on 2016/3/15.
 */
public class ReducerOutputValue implements Writable{
    FloatWritable product;
    IntWritable added;

    ReducerOutputValue(FloatWritable product, IntWritable added){
        this.product = product;
        this.added = added;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        product.write(dataOutput);
        added.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        product.readFields(dataInput);
        added.readFields(dataInput);
    }

    @Override
    public String toString() {
        return product.get() + "****" + added.get();
    }
}
