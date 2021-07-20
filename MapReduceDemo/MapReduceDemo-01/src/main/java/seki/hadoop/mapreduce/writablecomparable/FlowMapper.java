package seki.hadoop.mapreduce.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text, FlowBean,Text> {


    //只能对key进行排序，所以将FlowBean变为key
    private Text outV = new Text() ;
    private FlowBean outK = new FlowBean() ;
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
        outV.set( phone ) ;
        outK.setUpFlow(Long.parseLong(upFlow));
        outK.setDownFlow(Long.parseLong(downFlow));
        outK.setSumFlow();

        //5、写出数据
        context.write(outK,outV) ;
    }

}
