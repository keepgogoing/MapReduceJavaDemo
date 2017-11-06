package friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FofReducer  extends Reducer<Fof,IntWritable,Fof,IntWritable>{

    @Override
    protected void reduce(Fof key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        boolean f = true;
        for(IntWritable i:values){
            if(i.get() == 0){
                f=false;
                break;
            }else {
                sum=sum+i.get();
            }
        }
        if(f){
            context.write(key,new IntWritable(sum));
        }
    }
}
