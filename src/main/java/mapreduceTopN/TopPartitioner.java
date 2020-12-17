package mapreduceTopN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description
 * @Package mapreduceTopN
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/17 14:54
 */

public class TopPartitioner extends Partitioner<OrderBean, Text> {
    /**
     * @param orderBean K2
     * @param text      V2
     * @param i         ReduceTask的个数
     * @return 分区的编号
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int i) {
        // 按照K2中的订单编号分区
        return (orderBean.getOrderId().hashCode() & 2147483647) % i;
    }
}
