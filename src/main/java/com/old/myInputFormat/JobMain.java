package com.old.myInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @Description
 * @Package com.old.myInputFormat
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/17 10:07
 */

public class JobMain extends Configured implements Tool {
    String hdfsUri = "hdfs://192.168.168.10:9000";

    @Override
    public int run(String[] strings) throws Exception {
        // 1. 获取Job实例
        Job job = Job.getInstance(super.getConf(), "my_input_format");
        job.setJar("HadoopDemo-1.0-SNAPSHOT.jar");

        // 3. 设置读取文件使用的类和输入路径
        job.setInputFormatClass(MyInputFormat.class);
        MyInputFormat.addInputPath(job, new Path(hdfsUri + "/my_input_format_input"));

        // 4. 设置Mapper类和输出的键值对类型
        job.setMapperClass(MyInputFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 6. 设置输出的文件格式和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(hdfsUri + "/my_input_format_output"));
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsUri), new Configuration());
        Path outputPath = new Path(hdfsUri + "/my_input_format_output");
        boolean exists = fileSystem.exists(outputPath);
        if (exists) {
            fileSystem.delete(outputPath, true);
        }

        // 7. 等待程序执行完成
        boolean completion = job.waitForCompletion(true);
        return completion ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
