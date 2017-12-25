package com.wikisourceindex;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SaxHandler extends DefaultHandler {
    private String currentTag = null;
    private String currentValue = null;
    private String curTitle = null;
    private Boolean isInRevision = false;
    FileWriter fw = null;
    BufferedWriter bw = null;

    public SaxHandler(String output) {
        String path = output;
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
        try {
            file.createNewFile();
            fw = new FileWriter(file, true);
            bw = new BufferedWriter(fw);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String filter(String str) {
        str = str.toLowerCase();
        String regEx = "[`~!@#$%^&*()\\-+={}'\":;,\\[\\].<>/?￥…（）_|【】‘；：”“’。，、？\\s\\n]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.replaceAll(" ").trim();
    }

    public void outputToTxt(String value, String type) {
        try {
            if (type.equals("text")) {
                bw.write(filter(value) + " ");
            } else if (type.equals("id")) {
                if (value.equals("1")) {
                    bw.write(filter(value) + " ");
                }else{
                    bw.write("\n" + filter(value) + " ");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void startDocument() throws SAXException {
        // TODO 当读到一个开始标签的时候，会触发这个方法
        super.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
        // TODO 自动生成的方法存根
        super.endDocument();
    }

    @Override
    public void startElement(String uri, String localName, String name,
                             Attributes attributes) throws SAXException {
        // TODO 当遇到文档的开头的时候，调用这个方法
        super.startElement(uri, localName, name, attributes);
        if (name.equals("page")) {
            currentTag = name;
        } else if (name.equals("title")) {
            currentTag = name;
        } else if (name.equals("id")) {
            if (!isInRevision) {
                currentTag = name;
            }
        } else if (name.equals("revision")) {
            currentTag = name;
            isInRevision = true;
        } else if (name.equals("text")) {
            currentTag = name;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length)
            throws SAXException {
        // TODO 这个方法用来处理在XML文件中读到的内容
        super.characters(ch, start, length);

        if (currentTag != null) {
            currentValue = new String(ch, start, length);
            if (!currentValue.trim().equals("") && !currentValue.trim().equals("\n")) {
                if (currentTag.equals("id")) {
                    this.outputToTxt(currentValue, currentTag);
                } else if (currentTag.equals("text")) {
                    this.outputToTxt(currentValue, currentTag);
                }
            }
        }
    }

    @Override
    public void endElement(String uri, String localName, String name)
            throws SAXException {
        // TODO 在遇到结束标签的时候，调用这个方法
        super.endElement(uri, localName, name);
        if (name.equals("revision")) {
            isInRevision = false;
        } else if (name.equals("mediawiki")) {
            try {
                bw.flush();
                bw.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
