package summer.FlowCountSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author summer
 * @title: FlowCount
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-10 11:23
 * 将输出结果按照总流量降序排列
 */
public class FlowCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1 获取Job对象
        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        // 2 设置Jar包
        job.setJarByClass(FlowCount.class);
        // 3 关联Mapper和Reducer类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 4 设置Mapper输出的key和value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/summer/downloads/dataSet/phoneFlow.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/summer/downloads/HadoopOutput/phoneFlow"));
        // 7 提交Job
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
