package com.hadoop.hdfs;

public class CaseIgnoreWordCountMapper {
    public void map(String line, Context context) {
        String[] words = line.toLowerCase().split("\t");

        for(String word : words) {
            Object value = context.get(word);
            if(value == null) { // 表示没有出现过该单词
                context.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                context.write(word, v+1);  // 取出单词对应的次数+1
            }
        }

    }
}
