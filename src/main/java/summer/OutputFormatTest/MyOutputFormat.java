package summer.OutputFormatTest;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author summer
 * @title: MyOutputFormat
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-18 21:03
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) {
        return new MyOwnRecordWriter(job);
    }
}
