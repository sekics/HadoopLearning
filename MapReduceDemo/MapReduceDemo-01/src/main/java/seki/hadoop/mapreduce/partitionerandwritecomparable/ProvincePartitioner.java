package seki.hadoop.mapreduce.partitionerandwritecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean,Text>{
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String phone = text.toString() ;
        String prefix = phone.substring(0, 3);
        int partition = 4 ;
        if( "136".equals(prefix) ){
            partition = 0 ;
        }
        else if( "137".equals(prefix) ){
            partition = 1 ;
        }
        else if( "138".equals(prefix) ){
            partition = 2 ;
        }
        else if( "139".equals(prefix) ){
            partition = 3 ;
        }
        return partition ;
    }



    /*@Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //text为手机号
        String phone = text.toString();
        String prefix = phone.substring(0, 3);
        int partition = 4 ;
        if( "136".equals(prefix) ){
            partition = 0 ;
        }
        else if( "137".equals(prefix) ){
            partition = 1 ;
        }
        else if( "138".equals(prefix) ){
            partition = 2 ;
        }
        else if( "139".equals(prefix) ){
            partition = 3 ;
        }
        return partition ;
    }*/
}
