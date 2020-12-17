package mapreduceTopN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description
 * @Package mapreduceTopN
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/17 15:00
 */

/*
 * 1. 继承WritableComparator
 * 2. 调用父类的有参构造函数
 * 3. 指定分组的规则(重写方法)
 */
public class TopComparator  extends WritableComparator {
    public TopComparator(){
        // 2. 调用父类的有参构造函数
        super(OrderBean.class, true);
    }

    // 3. 指定分组的规则
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 3.1 对形参进行强制类型转换
        OrderBean first = (OrderBean)a;
        OrderBean second = (OrderBean)b;

        // 3.2 指定分区规则
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
