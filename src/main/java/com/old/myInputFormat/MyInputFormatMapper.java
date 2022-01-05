package com.old.myInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description
 * @Package com.old.myInputFormat
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/17 9:58
 */

public class MyInputFormatMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    /*
     * K1: NullWritable
     * V1: BytesWritable
     * K2: Text
     * V2: BytesWritable
     */
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // 1. 获取文件的名字作为K2
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName(); // K2

        // 2. V1就是V2
        // 3. 将K2, V2写入上下文
        context.write(new Text(fileName), value);
    }
}
