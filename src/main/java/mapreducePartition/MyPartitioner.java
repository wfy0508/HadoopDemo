package mapreducePartition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description
 * @Package mapreducePartition
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/12 21:43
 */

/*
 * 1. 定义分区规则
 * 2. 返回分区的编号
 */
public class MyPartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        // 1. 拆分文本数据，获取指定的字段值
        String[] split = text.toString().split("\t");
        String numString = split[5];
        // 2. 判断字段值和15的关系
        if (Integer.parseInt(numString) > 15) {
            return 1;
        } else {
            return 0;
        }
    }
}
