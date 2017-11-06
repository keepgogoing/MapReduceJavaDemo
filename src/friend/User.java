package friend;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class User implements WritableComparable<User> {
    private String uname;

    private int friendsCount;

    public User(){}

    public User(String uname,int friendsCount){
        this.uname = uname;
        this.friendsCount = friendsCount;
    }

    public String getUname() {
        return uname;
    }

    public void setUname(String uname) {
        this.uname = uname;
    }

    public int getFriendsCount() {
        return friendsCount;
    }

    public void setFriendsCount(int friendsCount) {
        this.friendsCount = friendsCount;
    }

    @Override
    public int compareTo(User o) {
        int result = this.uname.compareTo(o.getUname());
        if(result ==0){
            return Integer.compare(this.friendsCount,o.getFriendsCount());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(uname);
        dataOutput.writeInt(friendsCount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uname = dataInput.readUTF();
        this.friendsCount = dataInput.readInt();
    }
}
