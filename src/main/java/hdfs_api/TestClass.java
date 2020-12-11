package hdfs_api;

import java.util.Arrays;

/**
 * @Description
 * @Package hdfs_api
 * @Author wfy
 * @Version V1.0.0
 * @Date 2020/12/11 17:51
 */

public class TestClass {
    public static void main(String[] args) {
        String string = "The Apache Hadoop. software library, is a framework";
        String[] str = string.split("[,. ]+");
        System.out.println(Arrays.toString(str));
    }
}
