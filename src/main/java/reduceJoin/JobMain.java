package reduceJoin;

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
 * @Package ReduceJoin
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/15 15:59
 */

// Reduce段的join操作可能造成数据倾斜
public class JobMain extends Configured implements Tool {
    // 设置连接的服务器和端口
    String HdfsUri = "hdfs://192.168.168.10:9000";

    @Override
    public int run(String[] strings) throws Exception {
        // 1. 获取Job对象
        Job job = Job.getInstance(super.getConf(), "reduceJoin");
        job.setJar("HadoopDemo-1.0-SNAPSHOT.jar"); // 集群模式

        // 2. 文件读取使用的类和输入路径
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(HdfsUri + "/reduce_join_input");
        TextInputFormat.addInputPath(job, inputPath);

        // 3. 设置Mapper类和输出的键值对类型
        job.setMapperClass(ReduceJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 分区, 排序, 规约, 分组使用默认值

        // 4. 设置Reducer类和输出键值对类型
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 5. 设置输出类型和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        FileSystem fileSystem = FileSystem.get(URI.create(HdfsUri), new Configuration());
        Path outputPath = new Path(HdfsUri + "/reduce_join_output");
        boolean exists = fileSystem.exists(outputPath);
        if (exists) {
            fileSystem.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        // 6. 等待任务结束
        boolean completion = job.waitForCompletion(true);
        return completion ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
