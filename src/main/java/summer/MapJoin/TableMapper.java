package summer.MapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * @author summer
 * @title: TableMapper
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-20 15:33
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    /**
     * 定义一个map用于存放从内存表中读取的数据
     */
    private HashMap<String, String> pdMap = new HashMap<>();

    private Text outKey = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        // 获取一个文件系统
        FileSystem fs = FileSystem.get(context.getConfiguration());

        final FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        //从流中读取数据
        final BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 切割
            String[] split = line.split("\t");
            // 赋值
            pdMap.put(split[0], split[1]);
        }
        // 关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 获取order中一行
        String line = value.toString();
        // 切割
        final String[] split = line.split("\t");
        // 获取pName
        final String pName = pdMap.get(split[1]);
        outKey.set(split[0] + "\t" + pName + "\t" + split[2]);
        // 写出
        context.write(outKey, NullWritable.get());
    }
}
