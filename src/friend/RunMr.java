package friend;

import demo.WordCountMapper;
import demo.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RunMr {

    public static void main(String[] args){

        Configuration config = new Configuration();
        config.set("mapred.jar", "F:\\IdeaProject\\out\\artifacts\\IdeaProject_jar\\IdeaProject.jar");
        if(run1(config)){
            run2(config);
        }
    }

    public static boolean run1(Configuration config){

        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJarByClass(RunMr.class);

            job.setMapperClass(FofMapper.class);
            job.setReducerClass(FofReducer.class);

            job.setMapOutputKeyClass(Fof.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);

            FileInputFormat.addInputPath(job,new Path("/user/input/friend"));
            Path outpath = new Path("/user/f1");

            if(fs.exists(outpath)){
                fs.delete(outpath,true);
            }

            FileOutputFormat.setOutputPath(job,outpath);
            boolean f = job.waitForCompletion(true);

            if(f){
                System.out.println("job任务执行成功！");
                return f;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return  false;
    }

    public static void run2(Configuration config){

        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJarByClass(RunMr.class);

            job.setJobName("secondfof");

            job.setMapperClass(SortMapper.class);
            job.setReducerClass(SortReducer.class);

            job.setSortComparatorClass(FofSort.class);
            job.setGroupingComparatorClass(FofGroup.class);

            job.setMapOutputKeyClass(User.class);
            job.setMapOutputValueClass(User.class);


            job.setInputFormatClass(KeyValueTextInputFormat.class);

            FileInputFormat.addInputPath(job,new Path("/user/f1/part-r-00000"));
            Path outpath = new Path("/user/f2");

            if(fs.exists(outpath)){
                fs.delete(outpath,true);
            }

            FileOutputFormat.setOutputPath(job,outpath);
            boolean f = job.waitForCompletion(true);

            if(f){
                System.out.println("job任务执行成功！");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
