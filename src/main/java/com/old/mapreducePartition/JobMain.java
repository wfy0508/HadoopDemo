package com.old.mapreducePartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


/**
 * @Description
 * @Package com.old.mapreducePartition
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/12 21:53
 */

public class JobMain extends Configured implements Tool {
    // 指定连接的HDFS地址和端口
    String hdfsUri = "hdfs://192.168.168.10:9000";

    @Override
    public int run(String[] strings) throws Exception {
        // 1. 创建Job对象
        Job job = Job.getInstance(super.getConf(), "partition_reduce");
        job.setJar("HadoopDemo-1.0-SNAPSHOT.jar"); // 提交到YARN集群，需要指定jar包的名称，不然找不到Mapper和Reducer的class文件

        // 2. 对Job任务进行配置
        // 2.1 设置文件的读取路径和读取使用的类
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(hdfsUri + "/partition_input");
        TextInputFormat.addInputPath(job, inputPath);

        // 2.2 设置Mapper类和数据类型(K2, V2)
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 2.3 指定分区类
        job.setPartitionerClass(MyPartitioner.class);

        // 2.4, 2.5, 2.6使用默认值（排序，规约，分组）

        // 2.7 指定Reducer类和数据类型(K3, V3)
        job.setReducerClass(PartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置ReduceTask的个数
        job.setNumReduceTasks(2);

        // 2.8指定输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(hdfsUri + "/partition_output");
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration());
        boolean exists = fileSystem.exists(outputPath);
        if (exists) {
            fileSystem.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        // 3. 等待任务结束
        boolean completion = job.waitForCompletion(true);
        return completion ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 启动Job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
