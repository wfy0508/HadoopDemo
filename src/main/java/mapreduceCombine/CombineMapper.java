package mapreduceCombine;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Package mapreduceCombine
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/13 17:15
 */

public class CombineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    /*
     * key: K1（行偏移量）
     * value: V1（每一行的文本数据）
     * context: 上下文对象（给下一阶段提供数据）
     *
     * 如何将<K1, V1>转化为<K2, V2>
     * K1  V1
     * 0   hello, world, hadoop
     * 15  apache, hdfs, mapreduce
     * ----------------------------
     * K2        V2
     * hello     1
     * world     1
     * hadoop    1
     * apache    1
     * hdfs      1
     * mapreduce 1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 将每行的数据进行拆分
        String[] strings = value.toString().split("[,. ]+");

        // 2. 遍历数组，组装<K2, V2>
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        for (String string : strings) {
            text.set(string);
            longWritable.set(1);

            // 3. 将K2, V2写入上下文
            // context.write(new Text(string), new LongWritable(1)); // 避免循环每次都要创建一个新对象，先定义2个变量
            context.write(text, longWritable);
        }
    }
}
