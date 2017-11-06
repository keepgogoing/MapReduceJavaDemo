package demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    //该方法循环调用，从文件的split中读取每一行都会调用一次，把该行所在的下标为key，该行的内容为value
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words =  StringUtils.split(value.toString(),' ');
        for (String word:words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
