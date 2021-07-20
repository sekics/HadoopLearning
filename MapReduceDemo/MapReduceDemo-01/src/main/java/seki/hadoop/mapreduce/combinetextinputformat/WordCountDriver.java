package seki.hadoop.mapreduce.combinetextinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class  WordCountDriver {


    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{

        //1、获取Job
        Configuration conf = new Configuration() ;
        Job job = Job.getInstance(conf) ;

        //2、设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        //3、关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4、设置map输出的k - v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5、设置最终输出的k - v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果不设置FileInputFormat,默认使用TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
//
//        //虚拟存储最大值设置
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMaxInputSplitSize(job,20971502) ;

        //6、设置输入路径和输出路径
//        FileInputFormat.setInputPaths(job,new Path("D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputcombinetextinputformat\\a.txt"));
        FileInputFormat.addInputPaths(job, "D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputcombinetextinputformat\\a.txt");
        FileInputFormat.addInputPaths(job, "D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputcombinetextinputformat\\b.txt");
        FileInputFormat.addInputPaths(job, "D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputcombinetextinputformat\\c.txt");
        FileInputFormat.addInputPaths(job, "D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputcombinetextinputformat\\d.txt");
        FileOutputFormat.setOutputPath(job,new Path("D:\\ZJU_Course\\Hadoop\\output\\Combine"));

        //7、提交job
        boolean result = job.waitForCompletion(true) ;

        System.exit( result ? 0 : 1 );
    }
}
