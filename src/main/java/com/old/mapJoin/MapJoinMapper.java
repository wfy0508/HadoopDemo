package com.old.mapJoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @Description
 * @Package com.old.mapJoin
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/15 16:47
 */

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final HashMap<String, String> map = new HashMap<>();

    // 1. 将分布式小表数据读取到本地Map集合
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1.1 获取分布式文件列表
        URI[] cacheFiles = context.getCacheFiles();

        // 1.2 获取指定的分布式文件系统
        FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());

        // 1.3 获取文件输入流
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));

        // 1.4 读取文件内容，并将数据存入Map集合
        // 1.4.1 将字节输入流FSDataInputStream--->BufferedReader
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        // 1.4.2 读取小表文件内容
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split(",");
            map.put(split[0], line);
        }

        // 1.5 关闭流
        //bufferedReader.close();
        //fileSystem.close();
    }

    // 2. 处理大表，并实现大表和小表的关联
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 2.1 从行文本数据获取商品的ID， 得到K2
        String[] split = value.toString().split(",");
        String productId = split[2]; // K2

        // 2.2 在Map集合中，将商品的ID作为键，获取商品的行文本数据，将value和值拼接，得到V2
        String productLine = map.get(productId);
        String valueLine = productLine + "\t" + value.toString(); // V2

        // 2.3 将K2， V2写入上下文
        context.write(new Text(productId), new Text(valueLine));
    }
}
