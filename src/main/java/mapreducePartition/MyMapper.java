package mapreducePartition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Package mapreducePartition
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/12 21:38
 */

/*
 * K1: 行偏移量 LongWritable
 * V1: 一行的文本数据 Text
 * K2: 一行的文本数据
 * V2: NullWritable 不知道什么类型的空占位符
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 自定义一个计数器
        Counter counter = context.getCounter("MR_COUNTER", "partition_counter");
        // 每执行一次加1
        counter.increment(1L);
        context.write(value, NullWritable.get()); // 直接将V1赋值给K2，写入上下文
    }
}
