package weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RunMr {

    public static void main(String[] args){
        Configuration config = new Configuration();
        config.set("mapred.jar", "F:\\IdeaProject\\out\\artifacts\\weather\\IdeaProject_jar\\IdeaProject.jar");

        //这个用来自定义map分割的制表符，这句话的意思是 用逗号分割
        //		config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        try {
            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);
            job.setJarByClass(RunMr.class);
            job.setJobName("weather");

            job.setMapperClass(WeatherMapper.class);
            job.setReducerClass(WeatherReduce.class);

            job.setMapOutputKeyClass(MyKey.class);
            job.setMapOutputValueClass(DoubleWritable.class);


            job.setPartitionerClass(MyPartitioner.class);
            job.setSortComparatorClass(MySort.class);
            job.setGroupingComparatorClass(MyGrup.class);

            job.setNumReduceTasks(3);
            job.setInputFormatClass(KeyValueTextInputFormat.class);


            FileInputFormat.addInputPath(job,new Path("/user/input/weather.txt"));
            Path outpath = new Path("/user/output/weather");

            if(fs.exists(outpath)){
                fs.delete(outpath,true);
            }

            FileOutputFormat.setOutputPath(job,outpath);
            boolean f = job.waitForCompletion(true);

            if(f){
                System.out.println("job任务执行成功！");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}






