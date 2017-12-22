package com.wikisourceindex;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.Iterator;

public class XMLParser {

    private Document readXML() throws DocumentException {
        SAXReader reader = new SAXReader();
        return reader.read(new File("dataset/wikisourcetest.xml"));
    }
    private void parseXML(Document document) throws DocumentException {

        Element root = document.getRootElement();
        // iterate through child elements of root with element name "foo"
        for (Iterator<Element> pageIt = root.elementIterator("page"); pageIt.hasNext();) {
            Element page = pageIt.next();
            Element revision = page.element("revision");
            System.out.println(page.elementText("id"));
            System.out.println(revision.elementText("text"));
        }
    }

    public static void main(String[] args) {
        XMLParser xmlParser = new XMLParser();
        try{
            Document d = xmlParser.readXML();
            xmlParser.parseXML(d);
        }catch (DocumentException e){
            System.out.println("DocumentException:" + e.getMessage());
        }
    }
}