package summer.ReduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;


/**
 * @author summer
 * @title: TableReducer
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-20 10:56
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        // 这组数据会进入同一个reducer方法中
        // 01   1001    1   order
        // 01   1004    4   order
        // 01   Apple       pd

        // 准备两个集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        final TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if ("order".equals(value.getFlag())) {
                TableBean tmpTableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpTableBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(tmpTableBean);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        // 使用pd中的pName填充orderBean中的pName
        // 在这个阶段，大量的数据在Reduce阶段汇聚，如果有数据倾斜，会导致集群部分节点过于繁忙，降低集群性能
        // 由于pd.txt为产品维表，可以将其加载到内存中，在Map阶段与order.txt关联，减少或不在使用Reduce操作
        for (TableBean orderBean : orderBeans) {
            orderBean.setpName(pdBean.getpName());
            context.write(orderBean, NullWritable.get());
        }
    }
}
