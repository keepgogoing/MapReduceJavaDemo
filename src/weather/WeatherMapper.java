package weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

//每一行第一个隔开符左侧的为key，右边为value
class WeatherMapper extends Mapper<Text,Text,MyKey,DoubleWritable> {

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        try {
            Date date = format.parse(key.toString());
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int year = c.get(Calendar.YEAR);
            int month = c.get(Calendar.MONTH);

            double hot = Double.parseDouble(value.toString().substring(0,value.toString().lastIndexOf("c")));

            MyKey k = new MyKey();
            k.setYear(year);
            k.setMonth(month);
            k.setHot(hot);

            context.write(k,new DoubleWritable(hot));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}