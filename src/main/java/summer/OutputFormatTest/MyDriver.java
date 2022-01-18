package summer.OutputFormatTest;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * @author summer
 * @title: MyDriver
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-18 21:27
 */
public class MyDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MyDriver.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(MyOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/summer/downloads/test.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/summer/downloads/HadoopOutput/test"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
