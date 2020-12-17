package mapreduceTopN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Package mapreduceTopN
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/17 14:47
 */

/*
 * K1: LongWritable 行偏移量
 * V1: Text 一行的文本数据
 * K2: OrderBean 订单ID和订单金额的组合
 * V2: Text 一行的文本数据
 */
public class TopMapper extends Mapper<LongWritable, Text, OrderBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 拆分行文本数据，得到订单ID和订单金额
        String[] split = value.toString().split("\t");
        // 2. 封装OrderBean，得到K2
        OrderBean orderBean = new OrderBean();
        orderBean.setOrderId(split[0]);
        orderBean.setPrice(Double.valueOf(split[2]));
        // 3. 将K2，V2写入上下文
        context.write(orderBean, value);
    }
}
