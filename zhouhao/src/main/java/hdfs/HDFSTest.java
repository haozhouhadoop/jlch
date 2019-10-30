package hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Properties;

/**
 * Create by  ASUS on 2019/9/9.
 * description:
 */
public class HDFSTest {
    
    FileSystem fs = null;
    
    @Before
    public void init() throws Exception {
        
        //设置用户名，避免权限失败
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME","root");
//        properties.setProperty("hadoop.home.dir", "/opt/apps/hadoop/");
        
        //读取classpath下的xxx-site.xml 配置文件，并解析其内容，封装到conf对象中
        Configuration conf = new Configuration();
        
        //也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉配置文件中的读取的值
        conf.set("fs.defaultFS", "hdfs://192.168.43.100:9000/");
        
        fs = FileSystem.get(conf);
        //根据配置信息，去获取一个具体文件系统的客户端操作实例对象
        fs = FileSystem.get(new URI("hdfs://192.168.43.100:9000"), conf);
        
        
    }
    
    
    /**
     * 上传文件，比较底层的写法
     *
     * @throws Exception
     */
    @Test
    public void upload() throws Exception {
        
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.43.100:9000");
        
        FileSystem fs = FileSystem.get(conf);
        
        Path dst = new Path("hdfs://192.168.43.100:9000/zhouhao/qingshu.txt");
        
        FSDataOutputStream os = fs.create(dst);
        FileInputStream is = new FileInputStream("C:\\Users\\ASUS\\Desktop\\Scala001.class");
        
        IOUtils.copyBytes(is, os, 4096);
        
        
    }
    
    /**
     * 上传文件，封装好的写法
     *
     * @throws Exception
     * @throws IOException
     */
    @Test
    public void upload2() throws Exception, IOException {
        
        fs.copyFromLocalFile(new Path("C:\\Users\\ASUS\\Desktop\\Scala001.class"), new Path("hdfs://192.168.43.100:9000/qingshu2.txt"));
        
    }
    
    
    /**
     * 下载文件
     *
     * @throws Exception
     * @throws IllegalArgumentException
     */
    @Test
    public void download() throws Exception {
        
        //操作有问题
//        fs.copyToLocalFile(new Path("hdfs://192.168.43.100:9000/qingshu2.txt"), new Path("C:\\Users\\ASUS\\Desktop\\qingshu2.txt"));
        Path path = new Path("hdfs://192.168.43.100:9000/qingshu2.txt");
        FSDataInputStream fis = fs.open(path);
        OutputStream os = new BufferedOutputStream(new FileOutputStream("C:\\Users\\ASUS\\Desktop\\qingshu2.txt"));
        IOUtils.copyBytes(fis,os,1024,true);
        
    }
    
    /**
     * 查看文件信息
     *
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    @Test
    public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
        
        // listFiles列出的是文件信息，而且提供递归遍历
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        
        while (files.hasNext()) {
            
            LocatedFileStatus file = files.next();
            Path filePath = file.getPath();
            String fileName = filePath.getName();
            System.out.println(fileName);
            
        }
        
        System.out.println("---------------------------------");
        
        //listStatus 可以列出文件和文件夹的信息，但是不提供自带的递归遍历
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            
            String name = status.getPath().getName();
            System.out.println(name + (status.isDirectory() ? " is dir" : " is file"));
            
        }
        
    }
    
    /**
     * 创建文件夹
     *
     * @throws Exception
     * @throws IllegalArgumentException
     */
    @Test
    public void mkdir() throws IllegalArgumentException, Exception {
        
        fs.mkdirs(new Path("/aaa/bbb/ccc"));
        
        
    }
    
    /**
     * 删除文件或文件夹
     *
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Test
    public void rm() throws IllegalArgumentException, IOException {
        
        fs.delete(new Path("/aaa"), true);
        
    }
    
    
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://weekend110:9000/");
        
        FileSystem fs = FileSystem.get(conf);
        
        FSDataInputStream is = fs.open(new Path("/jdk-7u65-linux-i586.tar.gz"));
        
        FileOutputStream os = new FileOutputStream("c:/jdk7.tgz");
        
        IOUtils.copyBytes(is, os, 4096);
    }
    
    
}

