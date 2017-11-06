package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RunMr {

    public static void main(String[] args){
        Configuration config = new Configuration();
        config.set("mapred.jar", "F:\\IdeaProject\\out\\artifacts\\IdeaProject_jar\\IdeaProject.jar");
        try {
            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);
            job.setJarByClass(RunMr.class);
            job.setJobName("wordcount");
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job,new Path("/user/input/wc.txt"));
            Path outpath = new Path("/user/output");

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
