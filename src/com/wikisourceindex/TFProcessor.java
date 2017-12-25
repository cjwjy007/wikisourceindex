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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import static org.apache.commons.io.FileUtils.deleteDirectory;

public class TFProcessor {
    static String getWordCount() {
        File file = new File("write/count.txt");
        String wordCount = "";
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            reader.readLine();
            wordCount = reader.readLine();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wordCount.split(":")[1];
    }
    public static class TFProcessorMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Text k = new Text();
            Text v = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());
            itr.nextToken();
            while (itr.hasMoreTokens()) {
                String s = itr.nextToken();
                k.set(s);
                v.set(Integer.toString(1));
                context.write(k, v);
            }
        }
    }

    public static class TFProcessorReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text v = new Text();
            StringBuffer sb = new StringBuffer();
            long wordCount = Long.parseLong(getWordCount());
            long sum = 0;
            for (Text value : values) {
                sum += 1;
            }
            sb.append("sum:").append(sum).append(" tf:").append((double)sum/wordCount);
            v.set(sb.toString());
            context.write(key, v);
        }
    }

    public void runJob(String input, String output, Configuration conf) throws Exception {
        Job job = new Job(conf);
        job.setJarByClass(TFProcessor.class);

        job.setMapperClass(TFProcessorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(input));

        job.setReducerClass(TFProcessorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        File outf = new File(output);
        if (outf.exists()) {
            deleteDirectory(outf);//删除文件
        }
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        System.out.println("TF MR Finished");
    }
}