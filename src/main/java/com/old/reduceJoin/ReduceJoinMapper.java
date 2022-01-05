package com.old.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description
 * @Package ReduceJoin
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/15 15:30
 */

/*
 * K1 : LongWritable（行偏移量）
 * V1 : Text
 * K2 : Text（商品的ID）
 * V2 : Text (行文本信息，商品的信息)
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    /*
     * product.txt K1  V1
     *             0   p0001,iphone,5000,10000
     * orders.txt  K1  V1
     *             0   0001,20201212,p0001,2
     * ---------------------------------------
     * K2     V2
     * p0001  p0001,iphone,5000,10000
     * p0001  0001,20201212,p0001,2
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 判断数据来源文件
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if (fileName.equals("product.txt")) {
            // 数据来自商品表
            String[] productSplit = value.toString().split(",");
            String productId = productSplit[0];
            // 2. 将K1，V1转为K2，V2
            context.write(new Text(productId), value);

        } else {
            // 数据来自订单表
            String[] orderSplit = value.toString().split(",");
            String productId = orderSplit[2];
            // 2. 将K1，V1转为K2，V2
            context.write(new Text(productId), value);
        }
    }
}
