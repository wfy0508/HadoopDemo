package common_friends_step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package common_friends_step2
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/16 14:11
 */

public class Step2Reducer extends Reducer<Text, Text, Text, Text> {
    /*
     * K2    V2
     * A-C    <B,E>
     * A      C
     * A-B    <D,E>
     * B-C    E
     * B      F
     * ---------------
     * K3     V3
     * A-C    B-E
     * A      C
     * A-B    D-E
     * B-C    E
     * B      F
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 1. 原来的K2就是K3

        // 2. 遍历集合，拼接元素得到V3
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value).append("-");
        }
        builder.deleteCharAt(builder.length()-1);

        // 3. 将K3, V3写入上下文
        context.write(key, new Text(builder.toString()));
    }
}
