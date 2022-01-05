package com.old.reduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Package ReduceJoin
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/15 15:53
 */

public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 1. 拼接字符串
        String first = "";  // 拼接是商品在前
        StringBuilder second = new StringBuilder(); // 订单在后
        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                first = value.toString();
            } else {
                second.append(value.toString());
            }
        }
        // 2. 写入上下文中
        context.write(key, new Text(first + "\t" + second));
    }
}
