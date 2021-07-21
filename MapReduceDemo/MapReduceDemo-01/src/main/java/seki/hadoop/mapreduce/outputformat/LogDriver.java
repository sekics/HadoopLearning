package seki.hadoop.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import seki.hadoop.mapreduce.combinetextinputformat.WordCountDriver;
import seki.hadoop.mapreduce.combinetextinputformat.WordCountMapper;
import seki.hadoop.mapreduce.combinetextinputformat.WordCountReducer;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{

        //1、获取Job
        Configuration conf = new Configuration() ;
        Job job = Job.getInstance(conf) ;

        //2、设置jar包路径
        job.setJarByClass(LogDriver.class);

        //3、关联mapper和reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        //4、设置map输出的k - v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5、设置最终输出的k - v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //如果不设置FileInputFormat,默认使用TextInputFormat.class
//
        job.setOutputFormatClass(LogOuputFormat.class);

        //6、设置输入路径和输出路径
//        FileInputFormat.setInputPaths(job,new Path("D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputcombinetextinputformat\\a.txt"));
        FileInputFormat.addInputPaths(job, "D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputcombinetextinputformat\\d.txt");
        FileOutputFormat.setOutputPath(job,new Path("D:\\ZJU_Course\\Hadoop\\output\\Combine"));

        //7、提交job
        boolean result = job.waitForCompletion(true) ;

        System.exit( result ? 0 : 1 );
    }
}
