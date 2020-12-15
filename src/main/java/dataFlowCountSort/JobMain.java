package dataFlowCountSort;

import dataFlowCount.FlowBean;
import dataFlowCount.FlowMapper;
import dataFlowCount.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @Description
 * @Package dataFlowCountSort
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/14 11:22
 */

public class JobMain extends Configured implements Tool {
    // 设置连接的IP和端口
    String hdfsUri = "hdfs://192.168.168.10:9000";

    @Override
    public int run(String[] strings) throws Exception {
        // 1. 创建一个Job任务对象
        Job job = Job.getInstance(super.getConf(), "mapreduce_dataflow_count_sort");
        job.setJar("HadoopDemo-1.0-SNAPSHOT.jar"); //集群模式

        // 2. 设置Job任务的步骤（共8个步骤）
        // 2.1 指定文件读取使用的类和文件路径
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(hdfsUri + "/dataflow_output");
        TextInputFormat.addInputPath(job, inputPath);
        // 2.2 指定Mapper类和键值对类型
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);
        // 2.3（分区）, 2.4（排序）, 2.5（规约）, 2.6（分组）使用默认值
        // 2.7 指定Reducer类和输出键值对
        job.setReducerClass(SortReducer.class);
        // 2.8 指定输出键值对类型和输出路径
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsUri), new Configuration());
        Path outputPath = new Path(hdfsUri + "/dataflow_sort_output");
        boolean exists = fileSystem.exists(outputPath);
        if (exists) {
            fileSystem.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        // 3. 等待任务结束
        boolean completion = job.waitForCompletion(true);
        // 4. 返回
        return completion ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
