package friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<User,User,Text,Text> {
    @Override
    protected void reduce(User key, Iterable<User> values, Context context) throws IOException, InterruptedException {
        String user = key.getUname();
        StringBuffer sb = new StringBuffer();

        for(User u:values){
            sb.append(u.getUname()+":"+u.getFriendsCount());
            sb.append(",");
        }
        context.write(new Text(user),new Text(sb.toString()));
    }
}
