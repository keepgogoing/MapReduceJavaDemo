package weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyKey,DoubleWritable> {

    //maptask每输出一个数据的时候调用一次，因为执行的次数比较多，所以执行的时间越短越好
    @Override
    public int getPartition(MyKey key, DoubleWritable value, int numReduceTasks) {
       return (key.getYear() - 1949)%numReduceTasks;
    }
}
