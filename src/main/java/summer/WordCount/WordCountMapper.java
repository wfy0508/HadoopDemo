package summer.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author summer
 * @title: WordCountMapper
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-08 22:12
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 输出Key和输出Value
     */
    private final Text outKey = new Text();
    private final IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = line.split(" ,?.");
        for (String s : strings) {
            outKey.set(s);
            context.write(outKey, outValue);
        }
    }
}
