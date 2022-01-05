package com.old.mapreduceSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package com.old.mapreduceSort
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/13 13:03
 */

public class SortReducer extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {
    /**
     * reduce方法将K2, V2映射为K3, V3，直接传就可以了
     */
    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
