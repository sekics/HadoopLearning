package seki.hadoop.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //准备两个集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean() ;

        //遍历
        //hadoop里面的迭代器存的只有一个指针，如果每次add(value)最终只会有一个对象，因为value是一个固定的指针
        //每次改变的是value中的引用值，应该把该值取出来赋值给另外一个对象
        for (TableBean value : values) {
            if( "order".equals(value.getFlag()) ){
                TableBean tmpTableBean = new TableBean() ;
                try {
                    BeanUtils.copyProperties(tmpTableBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add( tmpTableBean ) ;
            }
            else{
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean orderBean : orderBeans) {
            orderBean.setPname( pdBean.getPname() );
            context.write(orderBean,NullWritable.get());
        }
    }
}
