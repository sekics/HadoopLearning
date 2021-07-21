package seki.hadoop.mapreduce.partitionerandwritecomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
* 自定义序列化对象
* 1、定义类实现Writable接口
* 2、重写序列化和反序列化方法
* 3、重写空参构造
* 4、重写toString()方法
* */
public class FlowBean implements WritableComparable<FlowBean> {

    private Long upFlow ;//上行流量
    private Long downFlow ;//下行流量
    private Long sumFlow ;//总流量

    //无参构造
    public FlowBean(){

    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow(){
        this.sumFlow = this.upFlow + this.downFlow ;
    }

    //重写序列化
    //写出和读入的顺序要一直，先写出upFlow就要先接受upFlow
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow) ;
        dataOutput.writeLong(downFlow) ;
        dataOutput.writeLong(sumFlow) ;
    }

    //重写反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong() ;
        this.downFlow = dataInput.readLong() ;
        this.sumFlow = dataInput.readLong() ;
    }

    @Override
    public String toString(){
        return this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow ;
    }

    @Override
    public int compareTo(FlowBean o) {
        if( this.sumFlow > o.sumFlow ){
            return -1 ;
        }
        else if( this.sumFlow < o.sumFlow ){
            return 1 ;
        }
        else{
            if( this.upFlow > o.upFlow ){
                return 1 ;
            }
            else if( this.upFlow < o.upFlow ){
                return -1 ;
            }
            else{
                return 0 ;
            }
        }
    }
}
