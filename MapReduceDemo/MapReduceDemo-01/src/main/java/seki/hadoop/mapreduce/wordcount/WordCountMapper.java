package seki.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
* org.apache.hadoop.mapreduce.对应 2.x 3.x的mapreduce，只负责计算
* org.apache.hadoop.mapred 对应1.x的mapreduce,负责资源调度和计算
* */
/*
* KEYIN:map阶段输入的key的类型，一般为偏移量，为LongWritable
* VALUEIN:map阶段输入的类型，为text
* KEYOUT:map阶段输出的类型，本例中为text
* VALUEOUT:map阶段输出的值，本例是单词出现的次数，为IntWritable
* */

public class WordCountMapper extends Mapper <LongWritable, Text,Text, IntWritable>{

    //重写map方法，每个键值对都会调用该方法

    //将变量定义在此处可以减少后续的调用过程中重复创建，因为每一行数据要调用一次map，map中的每个字符串也要调用一次
    private Text outK = new Text() ;
    private IntWritable outV = new IntWritable(1) ;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1、先获取一行数据
        String line = value.toString();

        //2、切割，将一行字符串使用空格分割得到一个字符串数组
        String[] words = line.split(" ") ;

        //3、循环写出
        for (String str : words){
            //不能直接传string类型，先把值传给Text
            outK.set(str);
            //写出数据给reducer
            context.write(outK,outV);
        }
    }
}
