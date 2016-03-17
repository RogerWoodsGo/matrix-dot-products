import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by beyondwu on 2016/3/15.
 */
public class MatrixReducer extends Reducer<LongWritable, FloatWritable, LongWritable, ReducerOutputValue> {
    @Override
    protected void reduce(LongWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        Iterator<FloatWritable> value = values.iterator();
        List<Float> twoMatrixValue = new ArrayList<Float>();

        while (value.hasNext()) {
            twoMatrixValue.add(value.next().get());
        }
        if(twoMatrixValue.size() == 2){
            context.write(key, new ReducerOutputValue(new FloatWritable(twoMatrixValue.get(0) * twoMatrixValue.get(1)), new IntWritable(1)));
        }
    }
}
