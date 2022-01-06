package summer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author summer
 * @title: HdfsClientTest
 * @projectName HadoopDemo
 * @description: TODO
 * @date 2022-01-05 14:42
 */
public class HdfsClientTest {

    private FileSystem fs;

    /**
     * 初始化连接
     */
    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {
        // 获取URI
        URI url = new URI("hdfs://node1:8020");
        // 配置信息
        Configuration configuration = new Configuration();
        // 登入hadoop的用户
        String user = "summer";
        // 获取一个客户端连接
        fs = FileSystem.get(url, configuration, user);
    }

    /**
     * 关闭连接
     */
    @After
    public void close() throws IOException {
        fs.close();
    }

    /**
     * 创建文件夹
     */
    @Test
    public void mkdirTest() throws IOException {
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    /**
     * 删除文件夹
     */
    @Test
    public void delDirTest() throws IOException {
        fs.delete(new Path("/xiyou/huaguoshan"), true);
    }

    /**
     * 上传文件
     */
    @Test
    public void putFileTest() throws IOException {
        Path src = new Path("/Users/summer/Downloads/upload/xsync.sh");
        Path dst = new Path("/xiyou");
        fs.copyFromLocalFile(src, dst);
    }

    /**
     * 下载文件
     */
    @Test
    public void pullFileTest() throws IOException {
        Path src = new Path("/xiyou/xsync.sh");
        Path dst = new Path("/Users/summer/Downloads/upload/");
        // delSrc: 是否删除源文件
        fs.copyToLocalFile(false, src, dst);
    }

    /**
     * 获取文件信息
     */
    @Test
    public void listFilesTest() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            System.out.println("-----------" + file.getPath() + "-----------");
            System.out.println("权限信息：" + file.getPermission());
            System.out.println("拥有者：" + file.getOwner());
            System.out.println("所属组：" + file.getGroup());
            System.out.println("长度：" + file.getLen());
            System.out.println("副本数：" + file.getReplication());
            System.out.println("文件名：" + file.getPath().getName());
            // 获取块信息
            BlockLocation[] locations = file.getBlockLocations();
            System.out.println(Arrays.toString(locations));

        }
    }
}
