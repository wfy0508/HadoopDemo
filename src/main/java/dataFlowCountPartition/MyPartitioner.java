package dataFlowCountPartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description
 * @Package dataFlowCountPartition
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/14 17:15
 */

public class MyPartitioner extends Partitioner<Text, FlowBean> {
    /*
     * 该方法用于指定分区规则
     * 139 开头数据到一个分区文件
     * 158 开头数据到一个分区文件
     * 199 开头数据到一个分区文件
     * 其他分区
     *
     * text: K2
     * flowSortBean：V2
     * i: ReduceTask的个数
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        // 1. 获取手机号
        String phoneNumber = text.toString();

        // 2. 判断手机号开头3位数，返回对应的分区编号
        if (phoneNumber.startsWith("135")) {
            return 0;
        } else if (phoneNumber.startsWith("158")) {
            return 1;
        } else if (phoneNumber.startsWith("199")) {
            return 2;
        } else {
            return 3;
        }
    }
}
