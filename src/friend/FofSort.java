package friend;

import org.apache.hadoop.io.WritableComparator;

public class FofSort extends WritableComparator {


    public FofSort(){
        super(User.class,true);

    }

    @Override
    public int compare(Object a, Object b) {
        User u1 = (User)a;
        User u2 = (User)b;

        int result = u1.getUname().compareTo(u2.getUname());
        if(result ==0){
            return -Integer.compare(u1.getFriendsCount(),u2.getFriendsCount());
        }
        return result;
    }
}
