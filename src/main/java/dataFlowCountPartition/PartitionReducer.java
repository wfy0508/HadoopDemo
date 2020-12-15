package dataFlowCountPartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package dataFlowCountSort
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/14 11:23
 */

public class PartitionReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    /*
     * key: K2
     * values: V2
     * context: 上下文
     *
     * 如何将K2, V2装换为K3, V3
     * K2           V2
     * 13979667793  FlowBean(20	179	90234	95330)
     * 13979667793  FlowBean(30	180	1000	5000)
     * -----------------
     * K3           V3
     * 13979667793  FlowBean(50	259	91234	100330)
     */

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 1. 遍历集合，将4个字段分别求和
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }

        // 2. 创建FlowBean对象，并将对象赋值
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);

        // 3. 将K3， V3写入上下文
        context.write(key, flowBean);
    }
}
