package com.hadoop.hdfs;

/*
*自定义wc实现类
* */
public class WordCountMapper implements MyMapper{

    public void map(String line, Context context ) {
        String[] words = line.split("\t");

        for(String word :words){
            Object value = context.get(word);
            if(value == null){
                context.write(word,1);
            }else{
                int v =Integer.parseInt(value.toString());
                context.write(word, v+1);//取出单词对应的次数+1
            }
        }
    }
}
