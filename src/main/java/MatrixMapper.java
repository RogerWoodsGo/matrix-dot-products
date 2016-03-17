import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Created by beyondwu on 2016/3/15.
 */
public class MatrixMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // super.map(key, value, context);
        String[] colValue = value.toString().split(",");
        //System.out.println(value + "****" + colValue[0] + "****" + colValue[1]);
        if (colValue.length > 1) {
            try {
                context.write(new LongWritable(Long.valueOf(colValue[0])), new FloatWritable((Float.valueOf(colValue[1]))));
            } catch (Exception e) {
                System.out.println("****" + value + "****");
            }
        }

    }
}





