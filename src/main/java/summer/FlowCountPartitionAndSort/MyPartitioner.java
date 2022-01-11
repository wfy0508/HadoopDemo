package summer.FlowCountPartitionAndSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author summer
 * @title: MyPartitioner
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-11 16:46
 */
public class MyPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        final String line = text.toString();
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
