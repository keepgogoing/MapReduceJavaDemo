package weather;

import org.apache.hadoop.io.WritableComparator;

public class MyGrup extends WritableComparator {

    public MyGrup(){
        super(MyKey.class,true);
    }

    //洗牌过程当中用来排序的，在reduce过程当中调用
    @Override
    public int compare(Object a, Object b) {
        MyKey k1 = (MyKey) a;
        MyKey k2 = (MyKey) b;

        int r1= Integer.compare(k1.getYear(),k2.getYear());
        if(r1 ==0){
           return Integer.compare(k1.getMonth(),k2.getMonth());
        }else{
            return r1;
        }
    }
}
