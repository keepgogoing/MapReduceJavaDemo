package friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FofMapper extends Mapper<Text,Text,Fof,IntWritable> {


    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String user = key.toString();
        String[] friends = StringUtils.split(value.toString(),'\t');

        for (int i=0;i<friends.length;i++){
            String f1 = friends[i];
            Fof ofof = new Fof(user,f1);
            context.write(ofof,new IntWritable(0));
            for(int j=i+1;j<friends.length;j++){
                String f2 = friends[j];
                Fof fof = new Fof(f1,f2);
                context.write(fof,new IntWritable(1));
            }
        }
    }

}
