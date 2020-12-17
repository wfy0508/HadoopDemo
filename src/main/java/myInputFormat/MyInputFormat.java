package myInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Description
 * @Package myInputFormat
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/16 17:05
 */

/*
 * 通过自定义MyInputFormat将文件读取后的<K1, V1>的格式设置为N<ullWritable, BytesWritable>
 */
public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 创建自定义RecordReader
        MyRecordReader myRecordReader = new MyRecordReader();
        // 将inputSplit，taskAttemptContext传给myRecordReader
        myRecordReader.initialize(inputSplit, taskAttemptContext);
        return myRecordReader;
    }

    // 文件是否被切割，因为是小文件合并，所以设置为不可切割，返回false
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
