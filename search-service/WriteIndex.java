import org.bson.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class WriteIndex {
    public static void main(String[] args) {
        try {
            MongoDBJDBC mongoDBJDBC = new MongoDBJDBC();
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/Users/wei/Downloads/enwikisource-20171020-pages-articles-multistream-index.txt")),
                    "UTF-8"));
            String lineTxt = null;
            int counter = 0;
            List<Document> documents = new ArrayList<Document>();
            while ((lineTxt = br.readLine()) != null) {
                String key = lineTxt.split(":")[1];
                String value = "";
                if (lineTxt.split(":").length == 4) {
                    value = lineTxt.split(":")[2] + ":" + lineTxt.split(":")[3];
                } else{
                    value = lineTxt.split(":")[2];
                }

//                System.out.println("key = " + key);
//                System.out.println("value = " + value);
                Document document = new Document("key", key).
                        append("value", value);
                counter++;
                documents.add(document);
                if (counter == 5) {
                    mongoDBJDBC.insertIndex(documents);
                    documents = new ArrayList<Document>();
                    counter = 0;
                }
            }
            mongoDBJDBC.insertIndex(documents);
            br.close();
        } catch (Exception e) {
            System.err.println("read errors :" + e);
        }
    }
}
