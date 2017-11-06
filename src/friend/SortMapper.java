package friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class SortMapper extends Mapper<Text,Text,User,User> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] string = StringUtils.split(value.toString(),'\t');
        //这个是fof的名称
        String other = string[0];
        int frientsCount = Integer.parseInt(string[1]);

        context.write(new User(key.toString(),frientsCount),new User(other,frientsCount));
        context.write(new User(other,frientsCount),new User(key.toString(),frientsCount));
    }
}
