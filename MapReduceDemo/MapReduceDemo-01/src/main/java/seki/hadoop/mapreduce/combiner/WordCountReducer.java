package seki.hadoop.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * KEYIN:reduce阶段输入的key的类型，是map阶段的输出健类型
 * VALUEIN:reduce阶段输入的类型，为map阶段的数值值类型
 * KEYOUT:reduce阶段输出的类型，本例中为text
 * VALUEOUT:reduce阶段输出的值，本例是单词出现的次数，为IntWritable
 * */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable outV = new IntWritable() ;
    //重写reduce方法,对每个键调用一次

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0 ;
        //value的数据   key : list
        // seki : (1,1,1) 对list中的数据进行累加
        for( IntWritable value : values ){
            sum += value.get() ;
        }

        outV.set(sum) ;

        context.write(key,outV) ;
    }
}
