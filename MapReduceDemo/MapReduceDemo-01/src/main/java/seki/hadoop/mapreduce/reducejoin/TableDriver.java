package seki.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import seki.hadoop.mapreduce.partitionerandwritecomparable.*;

import java.io.IOException;

public class TableDriver {

    public static void main(String[] args) throws IOException, InterruptedException ,ClassNotFoundException {

        // 1、获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置jar包
        job.setJarByClass(TableDriver.class);

        // 3、管理mapper 和 reducer
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        // 4、设置mapper 输出的k,v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 5、设置最终输出的k,v类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

    //        job.setPartitionerClass(ProvincePartitioner.class);
    //        job.setNumReduceTasks(5);

        // 6、设置数据输出和输入的路径
        FileInputFormat.addInputPaths(job,"D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputtable\\order.txt");
        FileInputFormat.addInputPaths(job,"D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputtable\\pd.txt");
        FileOutputFormat.setOutputPath(job,new Path("D:\\ZJU_Course\\Hadoop\\output\\Tableoutput"));

        // 7、提交job
        boolean res = job.waitForCompletion(true) ;
        System.exit( res ? 0 : 1 ) ;
    }
}
