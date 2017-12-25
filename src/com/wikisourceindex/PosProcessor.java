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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;

import static org.apache.commons.io.FileUtils.deleteDirectory;

public class PosProcessor {
    public static void writeCountToTxt(StringBuffer sb, int flag) throws IOException {
        File file = new File("write/count.txt");
        if (flag == 0) {
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();
        }
        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(sb.toString() + "\n");
        bw.flush();
        bw.close();
        fw.close();
    }

    public static class PosProcessorMapper extends Mapper<LongWritable, Text, Text, Text> {
        long docCount = 0;

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            Text k = new Text();
            Text v = new Text();
            int wordPos = 0;
            StringTokenizer itr = new StringTokenizer(value.toString());
            String id = itr.nextToken();
            docCount++;
            while (itr.hasMoreTokens()) {
                String s = itr.nextToken();
                k.set(s + "--" + id);
                v.set(Integer.toString(wordPos));
                wordPos++;
                context.write(k, v);
            }
        }

        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            sb.append("docCount:").append(docCount);
            writeCountToTxt(sb, 0);
        }
    }


    public static class PosProcessorReducer extends Reducer<Text, Text, Text, Text> {
        long wordCount = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text v = new Text();
            StringBuffer sb = new StringBuffer();
            for (Text value : values) {
                sb.append(value.toString()).append(" ");
                wordCount++;
            }
            v.set(sb.toString());
            context.write(key, v);
        }

        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            sb.append("wordCount:").append(wordCount);
            writeCountToTxt(sb, 1);
        }
    }

    public void runJob(String input, String output, Configuration conf) throws Exception {
        Job job = new Job(conf);
        job.setJarByClass(PosProcessor.class);

        job.setMapperClass(PosProcessorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(input));

        job.setReducerClass(PosProcessorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        File outf = new File(output);
        if (outf.exists()) {
            deleteDirectory(outf);//删除文件
        }
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        System.out.println("First MR Finished");
    }
}