package com.old.mapreduceCombine;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package com.old.mapreduceCombine
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/13 17:19
 */
// Combiner现将每个Mapper结束时做初步汇总，和Reducer的代码相同
public class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    /*
     * key: K2
     * values: V2
     * context: 上下文
     *
     * 如何将K2, V2装换为K3, V3
     * K2      V2
     * hello   <1, 1, 1>
     * world   <1, 1>
     * hadoop  <1>
     * -----------------
     * K3     V3
     * hello  3
     * world  2
     * hadoop 1
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 1. 遍历集合，将集合中的数字相加
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        // 2. 将K3和V3写入上下文
        context.write(key, new LongWritable(count));

    }
}
