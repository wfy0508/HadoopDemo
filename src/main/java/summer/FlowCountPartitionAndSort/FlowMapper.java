package summer.FlowCountPartitionAndSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author summer
 * @title: FlowMapper
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-10 11:22
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    /**
     * 输出key和value
     */
    private final FlowBean outKey = new FlowBean();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        // 获取一行
        final String line = value.toString();
        // 分割字符
        final String[] split = line.split("\t");
        // 提取对应数据
        String phone = split[1];
        String upFlow=split[split.length-2];
        String downFlow=split[split.length-1];
        // 封装
        outValue.set(phone);
        outKey.setUpFlow(Long.parseLong(upFlow));
        outKey.setDownFlow(Long.parseLong(downFlow));
        outKey.setSumFlow();
        // 写入到上下文中
        context.write(outKey, outValue);
    }
}
