package summer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
        fs.setTimes(dst, 0, 3000);
        fs.copyFromLocalFile(src, dst);
    }

    /**
     * 下载文件
     */
    @Test
    public void pullFileTest() throws IOException {
        Path src = new Path("/xiyou/xsync.sh");
        Path dst = new Path("/Users/summer/Downloads/upload/");
        fs.copyToLocalFile(true, src, dst);
    }
}
