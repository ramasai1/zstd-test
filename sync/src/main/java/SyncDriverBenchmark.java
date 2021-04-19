import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class SyncDriverBenchmark {
    private final MongoClient client;
    public long start, end;

    public SyncDriverBenchmark(String connectionUri) {
        this.client = MongoClients.create(connectionUri);
    }

    public void getAllDocs(String databaseName, String collectionName) {
        MongoCollection<Document> collection = this.client.getDatabase(databaseName)
                .getCollection(collectionName);

        this.start = System.currentTimeMillis();
        List<Document> allDocs = collection.find().into(new ArrayList<>());
        this.end = System.currentTimeMillis();
        System.out.println("TOTAL TIME TO FETCH WITH SYNC DRIVER: " + (this.end - this.start));
    }

    public void shutdownServer() {
        this.client.close();
    }
}
