package mapreducePartition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package mapreducePartition
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/12 21:50
 */

/*
 * K2: Text
 * V2: NullWriteable
 * K3: Text
 * V3: NullWriteable
 */
public class PartitionerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
