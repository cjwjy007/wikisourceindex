package com.wikisourceindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

import static org.apache.commons.io.FileUtils.deleteDirectory;

public class DFProcessor {
    public static class DFProcessorMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String input = value.toString().split(" ")[0];
            String word = input.split("--")[0];
            String doc = input.split("--")[1];
            Text k = new Text(word);
            Text v = new Text(doc);
            context.write(k, v);
        }
    }


    public static class DFProcessorReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text v = new Text();
            StringBuffer sb = new StringBuffer();
//            long docCount = Long.parseLong(Main.conf.get("docCount"));
            long sum = 0;
            for (Text value : values) {
                sum += 1;
            }
            sb.append("sum:").append(sum);
//            sb.append("sum:").append(sum).append(" df:").append((double)sum/docCount);
            v.set(sb.toString());
            context.write(key, v);
        }
    }

    public void runJob(String input, String output, Configuration conf) throws Exception {
        Job job = new Job(conf);
        job.setJarByClass(DFProcessor.class);

        job.setMapperClass(DFProcessorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(input));

        job.setReducerClass(DFProcessorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        File outf = new File(output);
        if(outf.exists()){
            deleteDirectory(outf);
        }
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        System.out.println("DF MR Finished");
    }
}