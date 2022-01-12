package summer.OutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author summer
 * @title: OutputFormatReducer
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-12 22:10
 */
public class OutputFormatReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
