package summer.OutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author summer
 * @title: LogOutputFormat
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-12 21:49
 * 自定义输出类，将包含summer的网址输出到summer.log，其他网址输出到other.log中
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        LogRecordWriter recordWriter = new LogRecordWriter(job);
        return recordWriter;
    }
}


