package seki.hadoop.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {


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

        //自定义Partitioner步骤
        //1 自定义继承partioner,重写getPartition方法
        //2 在Driver中设置自定义的partitioner
        //3 设置NumReduceTasks

        job.setNumReduceTasks(2);

        //6、设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\Code\\JavaCode\\IDEAPROJECT\\Hadoop\\input\\words.txt")) ;
//        FileInputFormat.setInputPaths(job,new Path("D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\ZJU_Course\\Hadoop\\output\\wordcountpartitioner1"));

        //7、提交job
        boolean result = job.waitForCompletion(true) ;

        System.exit( result ? 1 : 0 );
    }
}
