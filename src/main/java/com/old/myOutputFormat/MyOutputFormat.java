package com.old.myOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Package myInputOutputFormat
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/17 10:39
 */

public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 1. 获取目标文件的输出流(两个)
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        FSDataOutputStream goodOutputStream = fileSystem.create(new Path("hdfs://192.168.168.10:9000/good_comment/good.txt"));
        FSDataOutputStream badOutputStream = fileSystem.create(new Path("hdfs://192.168.168.10:9000/bad_comment/bad.txt"));

        // 2. 将输出流传给MyRecordWriter
        return new MyRecordWriter(goodOutputStream, badOutputStream);
    }
}
