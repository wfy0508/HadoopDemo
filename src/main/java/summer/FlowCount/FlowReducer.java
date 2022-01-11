package summer.FlowCount;

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
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outValue = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        // 定义上行流量和下行流量的累加值
        long totalUp = 0;
        long totalDown = 0;
        // 遍历集合求累加值
        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }
        // 封装输出
        outValue.setUpFlow(totalUp);
        outValue.setDownFlow(totalDown);
        outValue.setSumFlow();

        // 写入到输出
        context.write(key, outValue);
    }
}
