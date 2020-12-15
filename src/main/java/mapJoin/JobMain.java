package mapJoin;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Description
 * @Package mapJoin
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/15 16:47
 */

public class JobMain extends Configured implements Tool {
    String hdfsUri = "hdfs://192.168.168.10:9000";
    @Override
    public int run(String[] strings) throws Exception {
        // 1. 获取Job实例
        Job job = Job.getInstance(super.getConf(), "map_join");
        job.setJar("HadoopDemo-1.0-SNAPSHOT.jar");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
