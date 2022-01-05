package com.old.NCDCTemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package com.old.NCDCTemperature
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/11/19 14:11
 */

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            maxValue = Math.max(value.get(), maxValue);
        }
        context.write(key, new IntWritable(maxValue));
    }

}
