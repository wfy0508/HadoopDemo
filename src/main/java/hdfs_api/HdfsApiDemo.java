package hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @Description
 * @Package hdfs_api
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/9 17:37
 */

public class HdfsApiDemo {

    // 指定连接的HDFS地址和端口
    String hdfsUri = "hdfs://192.168.168.10:9000";

    @Test
    public void urlHdfs() throws IOException {
        // 1. 注册url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        // 2. 获取hdfs输入流
        InputStream inputStream = new URL("hdfsUri/a.txt").openStream();

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
        configuration.set("fs.defaultFS", hdfsUri);
        // 3. 获取执行的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        // 4. 输出
        System.out.println(fileSystem);
    }

    /*
    获取文件系统2
     */
    @Test
    public void getFileSystem2() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration());
        System.out.println(fileSystem);
    }

    /*
    获取文件系统3
     */
    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration = new Configuration();
        // 指定文件系统类型
        configuration.set("fs.defaultFS", hdfsUri);
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem);
    }

    /*
    获取文件系统4
     */
    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI(hdfsUri), new Configuration());
        System.out.println(fileSystem);
    }

    /*
    实现hdfs文件的遍历
     */
    @Test
    public void listFileSystem() throws URISyntaxException, IOException {
        // 1. 获取FileSystem文件系统
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration());

        // 2. 调用listFiles获取/目录下
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);

        // 3. 遍历迭代器
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            // 获取文件的绝对路径
            Path statusPath = fileStatus.getPath();
            System.out.println(statusPath + "----" + fileStatus.getPath().getName());
            // 获取文件的block信息
            int blockInfo = fileStatus.getBlockLocations().length;
            System.out.println(blockInfo);
        }
    }

    /*
    创建文件夹和文件
     */
    @Test
    public void mkdirs() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration());
        // 创建文件夹
        fileSystem.mkdirs(new Path("/hello"));
        // 创建一个文件
        fileSystem.create(new Path("/hello/a.txt"));
        fileSystem.close();
    }

    /*
    下载文件，方式1
     */
    @Test
    public void downloadFile1() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration());
        // 获取HDFS输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/hello/a.txt"));
        // 获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\hello.txt"));
        // 拷贝文件
        IOUtils.copy(inputStream, fileOutputStream);
        // 关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(fileOutputStream);
        fileSystem.close();
    }

    /*
    下载文件，方式2
     */
    @Test
    public void downloadFile2() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration());
        // 直接调用拷贝方法
        fileSystem.copyToLocalFile(new Path("/hello/a.txt"), new Path("D:\\hello.txt"));
        fileSystem.close();
    }

    /*
    上传文件
     */
    @Test
    public void uploadFile1() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration());
        // 直接调用拷贝方法
        fileSystem.copyFromLocalFile(new Path("D:\\test.txt"), new Path("/dir1"));
        fileSystem.close();
    }

    /*
    上传本地文件，并合并为大文件
     */
    @Test
    public void uploadAndMergeFile() throws URISyntaxException, IOException {
        // 获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(new URI(hdfsUri), new Configuration());
        // 创建一个输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hadoop_test/bigFile.txt"));
        // 获取本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        // 通过本地文件系统获取文件列表，为一个集合
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("D:\\hadoop_test\\little_files"));
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly();
        }
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }

}
