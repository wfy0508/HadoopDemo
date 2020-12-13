package mapreduceSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Package mapreduceSort
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/13 12:54
 */

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
    /*
     *
     * K1    V1
     * 0     a 3
     * 5     b 7
     * ---------
     * K2            V2
     * SortBean(a 3) NullWritable
     * SortBean(b 7) NullWritable
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 将行文本数据V1拆分，将数据封装到SortBean对象，就可以得到K2
        String[] split = value.toString().split("\t");
        SortBean sortBean = new SortBean();
        sortBean.setWord(split[0]);
        sortBean.setNum(Integer.parseInt(split[1]));

        // 2. 将K2, V2写入到上下文中
        context.write(sortBean, NullWritable.get());
    }
}
