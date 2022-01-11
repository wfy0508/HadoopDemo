package summer.FlowCountPartitionAndSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author summer
 * @title: FlowReducer
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-10 11:23
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    private final FlowBean outValue = new FlowBean();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
