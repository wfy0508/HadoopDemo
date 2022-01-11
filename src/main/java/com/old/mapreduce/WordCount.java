package com.old.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @Description
 * @Package com.old.mapreduce
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/11 15:44
 */

public class WordCount extends Configured implements Tool {
    /**
     * 指定连接的HDFS地址和端口
     */
    String hdfsUri = "hdfs://192.168.168.10:9000";

    /**
     * 该方法用于指定一个Job任务
     */
    @Override
    public int run(String[] strings) throws Exception {
        // 1. 创建一个Job任务对象
        Job job = Job.getInstance(super.getConf(), "wordCount");

        // 2. 设置Job任务步骤（8个阶段）
        // 2.1 指定读取文件使用的类和源文件路径
        // 提交到YARN集群，需要指定jar包的名称，不然找不到Mapper和Reducer的class文件
        job.setJar("HadoopDemo-1.0-SNAPSHOT.jar");
        // 指定读取文件使用的类
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(hdfsUri + "/wordcount");
        // 指定文件读取路径（集群模式）
        TextInputFormat.addInputPath(job, inputPath);

        // 指定文件读取路径（本地模式，要写成文件，不然会报错，应该是不支持多文件处理）
        //TextInputFormat.addInputPath(job, new Path("file:///D:\\hadoop_test\\input\\little1.txt"));


        // 2.2 指定map阶段使用的类和输出的键值对类型
        // 指定map阶段使用的类
        job.setMapperClass(WordCountMapper.class);
        // 设置map阶段K2的类型
        job.setMapOutputKeyClass(Text.class);
        // 设置map阶段V2的类型
        job.setMapOutputValueClass(LongWritable.class);

        // 2.3...2.6等4个阶段（分区，排序，规约，分组）使用默认值

        // 2.7 指定map阶段使用的类和输出的键值对类型
        // 指定reduce阶段使用的类
        job.setReducerClass(WordCountReducer.class);
        // 设置reduce阶段K3的类型
        job.setOutputKeyClass(Text.class);
        // 设置reduce阶段V3的类型
        job.setOutputValueClass(LongWritable.class);

        // 2.8 设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(hdfsUri + "/wc_output");

        // 获取一个文件系统
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration());
        boolean ifOutputPathExists = fileSystem.exists(outputPath);
        // 已存在该路径就删除（第二个参数true代表递归删除）
        if (ifOutputPathExists) {
            fileSystem.delete(outputPath, true);
        }

        //集群模式
        TextOutputFormat.setOutputPath(job, outputPath);
        //TextOutputFormat.setOutputPath(job, new Path("file:///D:\\hadoop_test\\output")); // 本地模式

        // 2.9 等待任务结束
        boolean completion = job.waitForCompletion(true);

        // 返回运行结果
        return completion ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 启动Job任务
        // 第二个参数需要一个Tool的实现类对象，WordCount类实现了这个Tool接口
        int run = ToolRunner.run(configuration, new WordCount(), args);
        System.exit(run);
    }


}
