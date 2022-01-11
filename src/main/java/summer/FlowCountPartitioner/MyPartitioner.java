package summer.FlowCountPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @author summer
 * @title: MyPartitioner
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-11 15:24
 * 按照手机号码前3位将手机流量输出到不同的分区
 */
public class MyPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String line = text.toString();
        String prePhone = line.substring(0, 3);
        int partition;
        switch (prePhone) {
            case "136":
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
                break;
            default:
                partition = 4;
        }
        return partition;
    }
}
