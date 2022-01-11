package summer.WordCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


/**
 * @author summer
 * @title: WordCount
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-08 22:12
 */
public class WordCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1 获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 2 设置jar包
        job.setJarByClass(WordCount.class);
        // 3 关联Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4 设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5 设置最终的<k, v>类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/summer/downloads/wordcount.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/summer/downloads/output"));
        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
