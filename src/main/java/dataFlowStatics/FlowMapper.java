package dataFlowStatics;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

/**
 * @Description
 * @Package dataFlowStatics
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/13 21:41
 */

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    /*
     * key: K1（行偏移量）
     * value: V1（每一行的文本数据）
     * context: 上下文对象（给下一阶段提供数据）
     *
     * 如何将<K1, V1>转化为<K2, V2>
     * K1  V1
     * 0   13979667793	20	179	90234	95330
     * ----------------------------
     * K2              V2
     * 13979667793     FlowBean(20	179	90234	95330)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 拆分行文本数据，得到手机号
        String[] split = value.toString().split("\t");
        String phoneNumber = split[0];
        // 2. 创建FlowBean对象，并拆分出流量字段赋值给FlowBean对象
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));
        // 3. 将K2，V2写入上下文中
        context.write(new Text(phoneNumber), flowBean);
    }
}
