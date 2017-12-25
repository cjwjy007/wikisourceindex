package com.wikisourceindex;

import org.apache.hadoop.conf.Configuration;

public class Main {
    public static void main(String[] args) throws Exception {
        String path[] = new String[5];
        path[0] = "/state/partition1/enwikisource-20171020-pages-articles-multistream.xml";
        path[1] = "write/processed.txt";
        path[2] = "write/pos";
        path[3] = "write/TF";
        path[4] = "write/DF";
//        if (args.length == 0) {
//            path[0] = "/state/partition1/enwikisource-20171020-pages-articles-multistream.xml";
//            path[1] = "write/processed.txt";
//            path[2] = "write/pos";
//            path[3] = "write/TF";
//            path[4] = "write/DF";
//        } else if (args.length == 1) {
//            path[1] = "write/processed.txt";
//            path[2] = "write/pos";
//            path[3] = "write/TF";
//            path[4] = "write/DF";
//        } else {
//            path = args;
//        }
        Configuration conf = new Configuration();
//        conf.set("fs.hdfs.impl",
//                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//        );
//        conf.set("fs.file.impl",
//                org.apache.hadoop.fs.LocalFileSystem.class.getName()
//        );
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
