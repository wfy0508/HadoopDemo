package summer.ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author summer
 * @title: TableMapper
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-20 10:39
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private final Text outKey = new Text();
    private final TableBean outValue = new TableBean();
    private String fileName;

    /**
     * 初始化获取文件名，只执行一次
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (fileName.contains("order")) {
            // 如果是订单表
            final String[] split = line.split("\t");
            outKey.set(split[1]);
            outValue.setId(split[0]);
            outValue.setPid(split[1]);
            outValue.setpName("");
            outValue.setAmount(Integer.parseInt(split[2]));
            outValue.setFlag("order");
        } else {
            // 处理的是pd.txt
            final String[] split = line.split("\t");
            outKey.set(split[0]);
            outValue.setId("");
            outValue.setPid(split[0]);
            outValue.setpName(split[1]);
            outValue.setAmount(0);
            outValue.setFlag("pd");
        }

        // 输出为
        // 01   1001    1   order
        // 01   1004    4   order
        // 01   Apple       pd
        // ...
        context.write(outKey, outValue);

    }
}
