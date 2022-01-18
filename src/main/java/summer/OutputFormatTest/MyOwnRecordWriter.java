package summer.OutputFormatTest;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author summer
 * @title: MyOwnRecordWriter
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-18 21:06
 */
public class MyOwnRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream myTest;
    private FSDataOutputStream myOther;

    public MyOwnRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            myTest = fs.create(new Path("/Users/summer/Downloads/HadoopOutput/MyTest.log"));
            myOther = fs.create(new Path("/Users/summer/Downloads/HadoopOutput/MyOthers.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String line = key.toString();
        if (line.contains("test")) {
            myTest.writeBytes(line + "\n");
        } else {
            myOther.writeBytes(line + "\n");
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        myTest.close();
        myOther.close();
    }
}
