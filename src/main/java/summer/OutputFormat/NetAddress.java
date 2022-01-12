package summer.OutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * @author summer
 * @title: NetAddress
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-12 22:14
 */
public class NetAddress {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1 获取Job对象
        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        // 2 设置Jar包
        job.setJarByClass(NetAddress.class);
        // 3 关联Mapper和Reducer类
        job.setMapperClass(OutputFormatMapper.class);
        job.setReducerClass(OutputFormatReducer.class);
        // 4 设置Mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 6 设置输出格式类
        job.setOutputFormatClass(LogOutputFormat.class);
        // 7 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/summer/downloads/dataSet/netAddress.txt"));
        // 虽然LogOutputFormat中已经定义了outputFormat，但是以为我们的outputFormat继承自FileOutputFormat
        // 而FileOutputFormat要输出一个_SUCCESS文件，所以这里还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("/Users/summer/downloads/HadoopOutput/netAddress"));
        // 8 提交Job
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
