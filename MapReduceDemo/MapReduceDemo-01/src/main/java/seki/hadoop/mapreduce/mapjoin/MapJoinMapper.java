package seki.hadoop.mapreduce.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Map<String,String> pdMap = new HashMap<>() ;
    private Text outK ;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存文件并将文件内容封装到map
        URI[] cacheFiles = context.getCacheFiles();

        FileSystem fs = FileSystem.get(context.getConfiguration()) ;
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0])) ;
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line ;
        while( ( line = reader.readLine() )!= null ){
            String[] split = line.split("\t");
            pdMap.put(split[0],split[1]) ;
        }

        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString() ;

        String[] split = line.split("\t");

        String rs = "" ;

        String pname = pdMap.get( split[1] ) ;

        rs = split[0] + "\t" + pname + "\t" + split[2] + "\t" ;

        outK.set(rs) ;
        context.write(outK,NullWritable.get());
    }
}
