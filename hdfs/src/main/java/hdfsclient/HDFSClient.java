package hdfsclient;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    private FileSystem fs;

    @Before
    public void before() throws InterruptedException, IOException{
        // 获取一个HDFS的抽象封装对象
        Configuration configuration = new Configuration();
        fs = FileSystem.get(URI.create("hdfs://nn1.hadoop:9000"), configuration, "hadoop");
        System.out.println(" before !!!");
    }

    @Test
    public void put() throws IOException{
        // 用这个对象操作文件系统
        fs.copyFromLocalFile(new Path("D:\\tmp\\input\\wordcount\\f1.txt"), new Path("/user/hadoop/mapreduce/output/"));
        // 关闭文件系统
        fs.close();
    }


    @Test
    public void get() throws IOException, InterruptedException{
        // 用这个对象操作文件系统
        fs.copyToLocalFile(new Path("/user/hadoop/mapreduce/input/innerjoin/m1.txt"), new Path("C:\\Users\\song\\Desktop\\output"));
    }


    @Test
    public void rename() throws IOException{
        // 用这个对象操作文件系统
        fs.rename(new Path("/user/hadoop/mapreduce/output/f1.txt"), new Path("/user/hadoop/mapreduce/output/f2.txt"));
    }


    @Test
    public void delete() throws IOException{
        // 用这个对象操作文件系统  true 递归删除
        boolean delete = fs.delete(new Path("/user/hadoop/mapreduce/output/f2.txt"), true);
        if(delete){
            System.out.println("删除成功 !!");
        }else{
            System.out.println("删除失败 !!");
        }
    }

    @Test
    public void append() throws IOException{
        // 读写
        FSDataOutputStream append = fs.append(new Path("/user/hadoop/mapreduce/output/f1.txt"), 1024);
        FileInputStream open = new FileInputStream("D:\\tmp\\input\\wordcount\\f1.txt");
        IOUtils.copyBytes(open, append, 1024, true);
    }

    @Test
    public void ls() throws IOException{
        // 列出当前文件夹
        FileStatus[] fileStatuses = fs.listStatus(new Path("/user/hadoop/mapreduce/output"));
        for (FileStatus fileStatus: fileStatuses){
            if (fileStatus.isFile()){
                System.out.println("这是一个文件: ");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
            }else {
                System.out.println("这是一个文件夹:");
                System.out.println(fileStatus.getPath());
            }
        }
    }

    @Test
    public void listFiles() throws IOException{
        // 只能查看文件，不能查看文件夹
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/user/hadoop/mapreduce/"), true);

        while(files.hasNext()){
            LocatedFileStatus file = files.next();
            System.out.println("================");
            System.out.println(file.getPath());

            System.out.println("快信息：");
            BlockLocation[] blockLocations = file.getBlockLocations();
            for(BlockLocation blockLocation : blockLocations){
                String[] hosts = blockLocation.getHosts();
                System.out.println("块在：");
                for(String host : hosts)
                    System.out.println(host + " ");
            }
        }
    }

    @After
    public void after() throws IOException{
        System.out.println("After !!!!!!");
        fs.close();
    }
}
