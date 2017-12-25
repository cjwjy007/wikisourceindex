package com.wikisourceindex;


import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;


public class SaxParser {
    public void parser(String input,String output){
        File file = new File(input);
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser parser =  factory.newSAXParser();
            SaxHandler handler = new SaxHandler(output);
            parser.parse(new FileInputStream(file), handler);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SaxParser saxParser = new SaxParser();
        saxParser.parser("/Users/wei/IdeaProjects/wikisourceindex/dataset/read/wikisourcetest.xml","write/processed.txt");
    }
}
