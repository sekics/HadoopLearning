package seki.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    //用来标记当前处理的文件的名称
    private String fileName ;
    private Text outK = new Text() ;
    private TableBean outV = new TableBean() ;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit() ;
        fileName = split.getPath().getName() ;

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行
        String line = value.toString() ;

        //判断是哪张表
        //处理订单表
        if( fileName.contains("order") ){

            String[] split = line.split("\t");
            //封装K,V
            outK.set(split[1]) ;
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        }
        else{
            String[] split = line.split("\t");
            outK.set(split[0]) ;
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }
        context.write(outK,outV);
    }
}
