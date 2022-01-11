package summer.FlowCount;

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
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    /**
     * 输出key和value
     */
    private Text outKey = new Text();
    private FlowBean outValue = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        // 使用\t分割
        String[] strings = line.split("\t");
        String phone = strings[1];
        String up = strings[strings.length - 2];
        String down = strings[strings.length - 1];

        // 封装
        outKey.set(phone);
        outValue.setUpFlow(Long.parseLong(up));
        outValue.setDownFlow(Long.parseLong(down));
        outValue.setSumFlow();
        // 输出
        context.write(outKey, outValue);
    }
}
