package friend;

import org.apache.hadoop.io.WritableComparator;

public class FofGroup extends WritableComparator {

    public FofGroup(){
        super(User.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        User u1 = (User)a;
        User u2 = (User)b;

        return u1.getUname().compareTo(u2.getUname());
    }
}
