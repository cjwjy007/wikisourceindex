package com.wikisourceindex;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XMLParser {
    private Document readXML(String input) throws Exception {
        SAXReader reader = new SAXReader();
        return reader.read(new File(input));
    }
    private void parseXML(Document document,String output) throws Exception, IOException {
        //input document
        Element root = document.getRootElement();
        //output document
        Document newDocument = DocumentHelper.createDocument();
        Element r = newDocument.addElement("mediawiki");
        //output to txt
        String path = output;
        File file = new File(path);
        if(file.exists()){
            file.delete();
        }
        file.createNewFile();
        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);

        for (Iterator<Element> pageIt = root.elementIterator("page"); pageIt.hasNext();) {
            Element page = pageIt.next();
            Element revision = page.element("revision");
            //input info
            String title = page.elementText("title").toLowerCase();
            String id = page.elementText("id");
            String text = revision.elementText("text").toLowerCase();
            //add to output
//            Element newPage = r.addElement("page");
//            newPage.addElement("id").addText(id);
//            newPage.addElement("title").addText(title);
//            newPage.addElement("text").addText(text);
            //write to text
            bw.write(id + " " + filter(title) + " " + filter(text) +"\n");
        }
//        writeXML(newDocument);
        bw.flush();
        bw.close();
        fw.close();
    }

    public String filter(String str) {
        String regEx = "[`~!@#$%^&*()\\-+={}'\":;,\\[\\].<>/?￥…（）_|【】‘；：”“’。，、？\\s\\n]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.replaceAll(" ").trim();
    }

    private void writeXML(Document document) throws IOException {
        FileWriter out = new FileWriter("");
        document.write(out);
        out.close();
    }

    void runXMLParser(String input, String output){
        try{
            Document d = this.readXML(input);
            this.parseXML(d,output);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}