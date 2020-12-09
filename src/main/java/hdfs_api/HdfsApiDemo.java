package hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.Test;

import java.io.*;
import java.net.URL;

/**
 * @Description
 * @Package hdfs_api
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/9 17:37
 */

public class HdfsApiDemo {
    @Test
    public void urlHdfs() throws IOException {
        // 1. 注册url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        // 2. 获取hdfs输入流
        InputStream inputStream = new URL("hdfs://192.168.168.10:9000/a.txt").openStream();

        // 3. 获取hdfs输出流
        OutputStream outputStream = new FileOutputStream(new File("D:\\hello.txt"));

        // 4. 实现文件的拷贝
        IOUtils.copy(inputStream, outputStream);

        // 5. 关流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }

    /*
    获取文件系统1
     */
    @Test
    public void getFileSystem1() throws IOException {
        // 1. 创建Configuration
        Configuration configuration = new Configuration();
        // 2. 设置文件系统类型
        configuration.set("fs.defaultFS", "hdfs://192.168.168.10:9000");
        // 3. 获取执行的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        // 4. 输出
        System.out.println(fileSystem);
    }
}
