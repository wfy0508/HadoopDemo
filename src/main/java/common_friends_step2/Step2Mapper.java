package common_friends_step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Description
 * @Package common_friends_step2
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/16 14:03
 */

public class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    /*
     * K1     V1
     * 0      A-C-    B
     * 1      A-      C
     * 2      A-B-    D
     * 3      A-C-B-  E
     * 4      B-      F
     * ---------------
     * K2    V2
     * A-C    B
     * A      C
     * A-B    D
     * A-B    E
     * B-C    E
     * A-C    E
     * B      F
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 拆分行文本数据得到V2
        String[] split = value.toString().split("\t");
        String friend_str = split[1];

        // 2. 继续以“-”为分隔符拆分数据的第一部分，得到数组
        String[] userArrays = split[0].split("-");

        // 3. 对数据进行排序，防止出现A-C，C-A重复的情况
        Arrays.sort(userArrays);

        /*
         * A-C-B   -----> A-B-C
         * 然后得到
         * A-B
         * A-C
         * B-C
         */
        // 4. 对数组中的数据进行两两组合，得到K2
        for (int i = 0; i < userArrays.length - 1; i++) {
            for (int j = i + 1; j < userArrays.length; j++) {
                // 5. 将K2, V2写入上下文
                context.write(new Text(userArrays[i] + "=" + userArrays[j]), new Text(friend_str));
            }
        }
    }
}
