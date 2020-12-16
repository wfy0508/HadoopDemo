package common_friends_step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package common_friends_step1
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/16 14:11
 */

public class Step1Reducer extends Reducer<Text, Text, Text, Text> {
    /*
     * K2    V2
     * B     <A,C>
     * C     <A>
     * D     <A,B>
     * E     <A,B,C>
     * F     <B>
     * ---------------
     * K3     V3
     * A-C    B
     * A      C
     * A-B    D
     * A-B-C  E
     * B      F
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 1. 遍历集合，将每个元素拼接，得到K3
        StringBuilder stringBuffer = new StringBuilder();
        for (Text value : values) {
            stringBuffer.append(value.toString()).append("-");
        }
        // 2. K2就是V3
        // 3. 将K3, V3写入上下文
        context.write(new Text(stringBuffer.toString()), key);
    }
}
