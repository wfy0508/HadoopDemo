package com.old.common_friends_step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Package com.old.common_friends_step1
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/16 14:03
 */

public class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * K1    V1
     * 0     A:B,C,D,E
     * 15    B:E,F,D
     * 20    C:E,B
     * ---------------
     * K2    V2
     * B     A
     * C     A
     * D     A
     * E     A
     * E     B
     * F     B
     * D     B
     * E     C
     * B     C
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 提取K2
        String[] split = value.toString().split(":");
        String userStr = split[0];
        // 2. 提取V2
        String[] split1 = split[1].split(",");
        for (String s : split1) {
            // 将K2, V2写入上下文中
            context.write(new Text(s), new Text(userStr));
        }
    }
}
