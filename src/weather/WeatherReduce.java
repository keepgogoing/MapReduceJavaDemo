package weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class WeatherReduce extends Reducer<MyKey,DoubleWritable,Text,NullWritable> {
    @Override
    protected void reduce(MyKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int i=0;
        for(DoubleWritable v:values){
            i++;
            String msg = key.getYear()+"/t"+key.getMonth()+"/t"+v.get();
            context.write(new Text(msg),NullWritable.get());
            if(i==3) break;
        }

    }
}