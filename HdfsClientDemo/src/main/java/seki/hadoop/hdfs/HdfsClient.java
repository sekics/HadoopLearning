package seki.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/*
* 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * HDFS zookeeper
* */
public class HdfsClient {

    private FileSystem fs ;

    @Before
    public void init() throws IOException, URISyntaxException,InterruptedException{

        //连接集群的NN地址 9870是web端地址
        URI uri = new URI("hdfs://hadoop102:8020") ;

        //创建一个配置文件
        Configuration configuration = new Configuration() ;

        String user = "seki" ;
        fs = FileSystem.get(uri,configuration,user) ;
    }

    @After
    public void close() throws IOException{
        fs.close();
    }

    @Test
    public void testMkdir() throws URISyntaxException,IOException,InterruptedException{
        fs.mkdirs(new Path("/Marvel/Avengers")) ;
    }

    /*
    * 参数优先级 hdfs-default.xml < hdfs-site.xml < 项目资源目录下的hdfs-site.xml < 代码里优先级配置
    * */
    @Test
    public void testPut() throws URISyntaxException,IOException,InterruptedException{
        /*
        *   参数解读：
        * 参数1：boolean delSrc 上传完成后是否删除本地文件
        * 参数2:boolean overwrite,如有重名是否覆盖
        * 参数3：数据源，参数4：目标地址
        * */
        fs.copyFromLocalFile(false,false,new Path("D:\\ZJU_Course\\Files\\Black.jpg"),new Path("hdfs://hadoop102//sanguo")) ;

    }

    @Test
    public void testGet() throws URISyntaxException,IOException,InterruptedException{
        /*
        * 参数解读
        *boolean delSrc 同上
        * Path src
        * Path des
        *boolean useRawLocalFileSystem 源文件校验传输
        * */
        fs.copyToLocalFile(false,new Path("hdfs://hadoop102//sanguo//Black.jpg"),new Path("D:\\ZJU_Course\\Hadoop\\testData"),true);
    }

    @Test
    public void testRemove() throws IOException{
        /*
        * 参数
        * 1 Path
        * 2 是否递归删除,空的目录为true或false都行，非空为true
        * */
        fs.delete(new Path("hdfs://hadoop102//sanguo//Black.jpg"),true) ;
    }

    @Test
    public void testMove() throws IOException{

        /*
        * 1 src
        * 2 des
        * */
        fs.rename(new Path("hdfs://hadoop102//sanguo//Black.jpg"),new Path("hdfs://hadoop102//sanguo//ZJU.jpg")) ;
    }

    //获取文件详细信息
    @Test
    public void testDetail() throws IOException{
        /*
        * 参数
        * 1 想要查看的文件信息
        * 2 是否递归
        * */
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true) ;

        //遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            //文件基本信息
            System.out.println("==================" + fileStatus.getPath() + "==================");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations() ;
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void testFile() throws IOException{
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("This is a file : "+fileStatus.getPath().getName());
            }
            else{
                System.out.println("This is a directory : " + fileStatus.getPath().getName());
            }
        }
    }
}
