import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class MongoDBJDBC {
    private MongoCollection<Document> wordCollection = null;
    private MongoCollection<Document> indexCollection = null;

    MongoDBJDBC() {
        connectToDB();
    }

    private void connectToDB() {
        // connect mongodb
        MongoClient mongoClient = new MongoClient("localhost", 27017);

        // connect to db
        MongoDatabase mongoDatabase = mongoClient.getDatabase("wikisource");
        System.out.println("Connect to database successfully");
        wordCollection = mongoDatabase.getCollection("source");
        indexCollection = mongoDatabase.getCollection("index");
    }

    List<String> searchWord(String word) {
        List<String> ret = new LinkedList<>();
        FindIterable<Document> findIterable = wordCollection.find(new Document("key", word));
        for (Document aFindIterable : findIterable) {
            String s = aFindIterable.get("value").toString();
            if(ret.indexOf(s) == -1){
                String title = searchIndex(s);
                if(!Objects.equals(title, "")){
                    ret.add(searchIndex(s));
                }
            }
            if(ret.size() > 8){
                return ret;
            }
        }
        return ret;
    }

    private String searchIndex(String index) {
        String title = "";
        FindIterable<Document> findIterable = indexCollection.find(new Document("key", index));
        for (Document aFindIterable : findIterable) {
            title = aFindIterable.get("value").toString();
        }
        return title;
    }

    void insertWord(List<Document> documents) {
        this.wordCollection.insertMany(documents);
    }

    void insertIndex(List<Document> documents) {
        this.indexCollection.insertMany(documents);
    }
}
