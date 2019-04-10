package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HDFSWCApp02 {
    public static void main(String[] args) throws Exception {

        // 1）读取HDFS上的文件 ==> HDFS API


        Properties properties = ParamsUtils.getProperties();
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));


        // 获取要操作的HDFS文件系统
        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)), new Configuration(),"hadoop");

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input, false);

        // TODO... 通过反射创建对象
        Class<?> clazz = Class.forName(properties.getProperty(Constants.MAPPER_CLASS));
        MyMapper mapper = (MyMapper)clazz.newInstance();


        //Mapper mapper = new WordCountMapper();
        Context context = new Context();

        while(iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = reader.readLine()) != null) {

                // 2）业务处理(词频统计)   (hello,3)
                // TODO... 在业务逻辑完之后将结果写到Cache中去
                mapper.map(line, context);
            }

            reader.close();
            in.close();
        }


        //TODO... 3 将处理结果缓存起来  Map

        Map<Object, Object> contextMap = context.getCacheMap();

        // 4）将结果输出到HDFS ==> HDFS API
        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));

        FSDataOutputStream out = fs.create(new Path(output, new Path(properties.getProperty(Constants.OUTPUT_FILE))));

        // TODO... 将第三步缓存中的内容输出到out中去
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for(Map.Entry<Object, Object> entry : entries) {
            out.write((entry.getKey().toString() + " \t " + entry.getValue() + "\n").getBytes());
        }

        out.close();
        fs.close();

        System.out.println("HDFS API统计词频运行成功....");

    }
}
