package seki.hadoop.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguOut ;
    private FSDataOutputStream otherOut ;

    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        //创建两条流
        try{

            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            atguiguOut = fs.create(new Path("D:\\ZJU_Course\\Hadoop\\output\\outputformat\\atguigu.log"));
            otherOut = fs.create(new Path(("D:\\ZJU_Course\\Hadoop\\output\\outputformat\\other.lpg"))) ;

        }
        catch (Exception e ){
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = text.toString();
        if( log.contains("atguigu")){
            atguiguOut.writeBytes(log + "\n" );
        }
        else{
            otherOut.writeBytes(log + "\n" );
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关闭流
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
