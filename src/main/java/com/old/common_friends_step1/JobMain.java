package com.old.common_friends_step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @Description
 * @Package com.old.common_friends_step1
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/16 14:22
 */

public class JobMain extends Configured implements Tool {
    String hdfsUri = "hdfs://192.168.168.10:9000";

    @Override
    public int run(String[] strings) throws Exception {
        // 1. 获取Job实例
        Job job = Job.getInstance(super.getConf(), "map_join");
        job.setJar("HadoopDemo-1.0-SNAPSHOT.jar");

        // 3. 设置读取文件使用的类和输入路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(hdfsUri + "/step1_input"));

        // 4. 设置Mapper类和输出的键值对类型
        job.setMapperClass(Step1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5. 设置Reducer类和输出的键值对类型
        job.setReducerClass(Step1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6. 设置输出的文件格式和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(hdfsUri + "/step1_output"));
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsUri), new Configuration());
        Path outputPath = new Path(hdfsUri + "/step1_output");
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
