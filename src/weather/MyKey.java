package weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {
    private int year;
    private int month;
    private double hot;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public double getHot() {
        return hot;
    }

    public void setHot(double hot) {
        this.hot = hot;
    }

    @Override
    public int compareTo(MyKey o) {

        int r1= Integer.compare(this.year,o.getYear());
        if(r1 ==0){
            int r2=Integer.compare(this.month,o.getMonth());
            if(r2==0){
                return Double.compare(this.hot,o.getHot());
            }else{
                return r2;
            }
        }else{
            return r1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeDouble(hot);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year=dataInput.readInt();
        this.month=dataInput.readInt();
        this.hot=dataInput.readDouble();
    }
}
