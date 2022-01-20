package summer.ReduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author summer
 * @title: TableJoin
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-20 11:18
 */
public class TableJoin {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf);

        job.setJarByClass(TableJoin.class);

        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/summer/Downloads/dataSet/inputTable/"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/summer/Downloads/HadoopOutput/tableOutput2"));

        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
