package seki.hadoop.mapreduce.partitioner2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException , InterruptedException ,ClassNotFoundException {

        // 1、获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置jar包
        job.setJarByClass(FlowDriver.class);

        // 3、管理mapper 和 reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4、设置mapper 输出的k,v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5、设置最终输出的k,v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        // 6、设置数据输出和输入的路径
        FileInputFormat.setInputPaths(job,new Path("D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputflow\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\ZJU_Course\\Hadoop\\output\\FlowOutput1"));

        // 7、提交job
        boolean res = job.waitForCompletion(true) ;
        System.exit( res ? 0 : 1 ) ;
    }
}
