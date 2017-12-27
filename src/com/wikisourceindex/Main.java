package com.wikisourceindex;

import org.apache.hadoop.conf.Configuration;

public class Main {
    public static final Configuration conf = new Configuration();
    public static void main(String[] args) throws Exception {
        String path[] = new String[5];
        path[0] = "/state/partition1/enwikisource-20171020-pages-articles-multistream.xml";
        path[1] = "/user/14307130205/write/processed.txt";
        path[2] = "/user/14307130205/write/pos";
        path[3] = "/user/14307130205/write/TF";
        path[4] = "/user/14307130205/write/DF";

        //test
//        path[0] = "/Users/wei/IdeaProjects/wikisourceindex/dataset/read/wikisourcetest.xml";
//        path[1] = "write/processed.txt";
//        path[2] = "write/pos";
//        path[3] = "write/TF";
//        path[4] = "write/DF";


        SaxParser saxParser = new SaxParser();
        saxParser.parser(path[0],path[1]);
        PosProcessor posProcessor = new PosProcessor();
        posProcessor.runJob(path[1], path[2], conf);
        TFProcessor tfProcessor = new TFProcessor();
        tfProcessor.runJob(path[1], path[3], conf);
        DFProcessor dfProcessor = new DFProcessor();
        dfProcessor.runJob(path[2], path[4], conf);
    }
}
