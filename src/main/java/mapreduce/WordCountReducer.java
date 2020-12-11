package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package mapreduce
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/11 16:21
 */

/*
 * Text : K2
 * LongWritable: V2
 * Text: K3
 * LongWritable: V3
 *
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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
        long count = 1;
        for (LongWritable value: values){
            count += value.get();
        }
        // 2. 将K3， V2写入到上下文中
        context.write(key, new LongWritable(count));
    }
}
