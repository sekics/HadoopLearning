package seki.hadoop.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Configuration conf = new Configuration( );
        Job job = Job.getInstance(conf) ;

        job.setJarByClass(MapJoinDriver.class) ;

        job.setMapperClass(MapJoinMapper.class) ;


        job.setMapOutputKeyClass(Text.class) ;
        job.setMapOutputValueClass(NullWritable.class) ;

        job.setOutputKeyClass(Text.class) ;
        job.setOutputValueClass(NullWritable.class) ;

        job.addCacheFile(new URI("file:///D:/ZJU_Course/Hadoop/资料/11_input/inputtable/pd.txt"));
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPaths(job,"D:\\ZJU_Course\\Hadoop\\资料\\11_input\\inputtable\\order.txt");
        FileOutputFormat.setOutputPath(job,new Path("D:\\ZJU_Course\\Hadoop\\output\\Tableoutput"));

        boolean res = job.waitForCompletion(true) ;
        System.exit( res ? 0 : 1 ) ;
    }
}
