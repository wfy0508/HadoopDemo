package summer.OutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author summer
 * @title: LogRecordWriter
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-12 21:51
 */

class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream summerOut;
    private FSDataOutputStream othOut;

    /**
     * 构造方法
     */
    public LogRecordWriter(TaskAttemptContext job) {
        // 创建两条流
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            summerOut = fs.create(new Path("/Users/summer/Downloads/HadoopOutput/summer.log"));
            othOut = fs.create(new Path("/Users/summer/Downloads/HadoopOutput/others.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String line = key.toString();
        // 写出
        if (line.contains("summer")) {
            summerOut.writeBytes(line + "\n");
        } else {
            othOut.writeBytes(line + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // 关闭流
        IOUtils.closeStream(summerOut);
        IOUtils.closeStream(othOut);
    }
}
