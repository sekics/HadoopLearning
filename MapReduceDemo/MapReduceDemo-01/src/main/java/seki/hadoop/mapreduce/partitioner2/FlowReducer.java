package seki.hadoop.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {

    private FlowBean outV = new FlowBean() ;
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //遍历key对应的values
        long totalUp = 0 ;
        long totalDown = 0 ;
        for (FlowBean value : values) {
            totalUp += value.getUpFlow() ;
            totalDown += value.getDownFlow() ;
        }

        //封装输出键值对
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        //将键值对写出
        context.write(key,outV);
    }
}
