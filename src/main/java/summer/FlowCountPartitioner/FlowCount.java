package summer.FlowCountPartitioner;

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
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置分区类和Reducer的数量
        job.setPartitionerClass(MyPartitioner.class);
        // 如果设置为1，走默认分区类，不会走自定义Partitioner
        // 如果设置小于自定义分区数量，则会报错
        // 如果设置大于自定义分区数量，则多出来的分区输出为空
        job.setNumReduceTasks(5);

        // 5 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/summer/downloads/phoneFlow.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/summer/downloads/phoneFlow1"));
        // 7 提交Job
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
