package com.old.myInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description
 * @Package com.old.myInputFormat
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/16 17:45
 */

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private Configuration configuration = null; // 定义configuration对象
    private FileSplit fileSplit = null; // 定义读取的源文件切片
    private boolean processed = false; // 定义文件读取标志位
    private final BytesWritable bytesWritable = new BytesWritable();
    private FileSystem fileSystem = null;
    private FSDataInputStream inputStream = null;

    // 进行初始化工作
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 获取文件的切片
        fileSplit = (FileSplit) inputSplit;
        // 获取configuration对象
        configuration = taskAttemptContext.getConfiguration();
    }

    // 用于获取K1, V1
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            // 1. 获取源文件的输入字节流
            // 1.1 获取文件系统
            fileSystem = FileSystem.get(configuration);
            // 1.2 通过文件系统获取输入数据的字节流
            inputStream = fileSystem.open(fileSplit.getPath());

            // 2. 读取源文件数据到普通的字节数组（byte[]）
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(inputStream, bytes, 0, (int) fileSplit.getLength());

            // 3. 将字节数组封装到BytesWritable，得到V1
            bytesWritable.set(bytes, 0, (int) fileSplit.getLength());

            processed = true;
            return true;
        }
        return false;
    }

    // 返回K1
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    // 返回V1
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    // 获取文件读取的进度
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    // 资源释放
    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();
    }
}
