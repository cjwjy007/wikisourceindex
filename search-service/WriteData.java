import org.bson.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class WriteData {
    public static void main(String[] args) {
        try {
            MongoDBJDBC mongoDBJDBC = new MongoDBJDBC();
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/Users/wei/Downloads/part-r-00000")),
                    "UTF-8"));
            String lineTxt = null;
            int counter = 0;
            List<Document> documents = new ArrayList<Document>();
            while ((lineTxt = br.readLine()) != null) {
                String text = lineTxt.split("\t")[0];
                String key = text.split("--")[0];
                String value = text.split("--")[1];
//                System.out.println("key = " + key);
//                System.out.println("value = " + value);
                Document document = new Document("key", key).
                        append("value", value);
                counter++;
                documents.add(document);
                if(counter == 1000){
                    mongoDBJDBC.insertWord(documents);
                    documents = new ArrayList<Document>();
                    counter = 0;
                }
            }
            mongoDBJDBC.insertWord(documents);
            br.close();
        } catch (Exception e) {
            System.err.println("read errors :" + e);
        }

    }
}
