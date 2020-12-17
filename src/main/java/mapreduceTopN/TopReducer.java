package mapreduceTopN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package mapreduceTopN
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/17 15:23
 */

public class TopReducer extends Reducer<OrderBean, Text, Text, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (Text value : values) {
            context.write(value, NullWritable.get());
            i++;
            if (i > 1) { // Top1
                break;
            }
        }
    }
}
