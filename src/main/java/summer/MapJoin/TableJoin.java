package summer.MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author summer
 * @title: TableJoin
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-20 11:18
 */
public class TableJoin {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf);

        job.setJarByClass(TableJoin.class);

        job.setMapperClass(TableMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 将pd.txt加载到内存中
        job.addCacheFile(new URI("/Users/summer/Downloads/dataSet/inputTable/pd.txt"));

        // 不使用Reduce阶段
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/summer/Downloads/dataSet/inputTable/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/summer/Downloads/HadoopOutput/tableOutput3"));

        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
