package seki.hadoop.mapreduce.wtitable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text, Text,FlowBean> {

    private Text outK = new Text() ;
    private FlowBean outV = new FlowBean() ;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1、获取一行
        String line = value.toString() ;

        //2、切割
        String[] words = line.split("\t") ;

        //3、截取需要的数据
        String phone = words[1] ;
        String upFlow = words[ words.length - 3 ] ;
        String downFlow = words[ words.length - 2 ] ;
        String sumFlow = words[words.length - 1 ] ;

        //4、封装数据
        outK.set( phone ) ;
        outV.setUpFlow(Long.parseLong(upFlow));
        outV.setDownFlow(Long.parseLong(downFlow));
        outV.setSumFlow();

        //5、写出数据
        context.write(outK,outV) ;
    }

}
