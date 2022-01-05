package com.old.dataFlowCountSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package com.old.dataFlowCountSort
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/14 11:23
 */

public class SortReducer extends Reducer<FlowSortBean, Text, Text, FlowSortBean> {
    /*
     * key: K2
     * values: V2
     * context: 上下文
     *
     * 如何将K2, V2装换为K3, V3
     * K2                               V2
     * FlowBean(20	179	90234	95330)  13979667793
     * -----------------
     * K3           V3
     * 13979667793  FlowBean(50	259	91234	100330)
     */

    @Override
    protected void reduce(FlowSortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
