package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


//使用hdfs api完成wordcount
/*
*需求：统计hdfs上的文件的wc，然后将统计结果输出到hdfs
* 功能拆解：
* 1.读取hdfs上的文件
* 2.业务处理（词频统计），对文件中的每一行数据都要进行业务处理
* 3.将业务结果缓存起来 context
* 4.将结果输出到HDFS
*
* */
public class HDFSWCApp01 {
    public static void main (String[] args) throws Exception {
        //1.读取hdfs上的文件
        Path input = new Path("/hdfsapi/test/hello.txt");

        //获取要操作的HDFS文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop000:8080"),new Configuration(),"hadoop");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);

        MyMapper mapper = new WordCountMapper();
        Context context = new Context();

        while(iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while((line= reader.readLine())!=null){
                //2.业务处理，词频统计
                //在业务逻辑处理后写道缓存中去
                mapper.map(line, context);
            }
            reader.close();
            in.close();
        }

        //3.将业务结果缓存起来
        Map<Object, Object> contextMap = new HashMap<Object, Object>();

        //4.将结果输出到hdfs
        Path output = new Path("/hdfsapi/output");

        FSDataOutputStream out = fs.create(new Path(output, new Path("wc.out")));

        //将第三步缓存中的内容输出到output中去
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for(Map.Entry<Object, Object> entry:entries){
            out.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());

        }
        out.close();
        fs.close();
        System.out.println("统计词频运行成功");

    }
}
