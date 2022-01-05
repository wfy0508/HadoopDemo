package com.old.myOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Description
 * @Package myInputOutputFormat
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/17 10:42
 */

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream goodOutputStream = null;
    private FSDataOutputStream badOutputStream = null;

    public MyRecordWriter() {

    }

    public MyRecordWriter(FSDataOutputStream goodOutputStream, FSDataOutputStream badOutputStream) {
        this.goodOutputStream = goodOutputStream;
        this.badOutputStream = badOutputStream;
    }

    /**
     * @param text         行文本内容
     * @param nullWritable
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        // 1. 获取文件中的好评字段
        String[] split = text.toString().split("\t");
        String numStr = split[2];

        // 2. 根据字段值得不同，写入不同的文件中
        if (Integer.parseInt(numStr) == 0) {
            // 好评
            goodOutputStream.write(text.toString().getBytes());
            goodOutputStream.write("\r\n".getBytes());
        } else {
            // 中评或差评
            badOutputStream.write(text.toString().getBytes());
            badOutputStream.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStreams(goodOutputStream, badOutputStream);
    }
}
