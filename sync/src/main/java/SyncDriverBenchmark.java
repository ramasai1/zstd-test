import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SyncDriverBenchmark {
    private final MongoClient client;
    private final String compressionType;

    public SyncDriverBenchmark(String connectionUri, String compressionType) {
        this.client = MongoClients.create(connectionUri);
        this.compressionType = compressionType;
    }

    public void getAllDocs(String dbName, String collectionName) {
        MongoCollection<Document> collection = this.client.getDatabase(dbName)
                .getCollection(collectionName);

        long start = System.currentTimeMillis();
        List<Document> allDocs = collection.find().into(new ArrayList<>());
        long end = System.currentTimeMillis();
        System.out.println(String.format("Time taken to read using %s compression: %d",
                this.compressionType, (end - start)));
    }

    public void write(String dbName, String collectionName, String path) {
        ArrayList<InsertOneModel<Document>> docs = new ArrayList<>();
        MongoCollection<Document> collection = this.client.getDatabase(dbName)
                .getCollection(collectionName);
        int count = 0, batch = 10000;

        collection.drop();
        long start = System.currentTimeMillis();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                docs.add(new InsertOneModel<>(Document.parse(line)));
                count++;
                if (count == batch) {
                    collection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
                    docs.clear();
                    count = 0;
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("file path is wrong");
        } catch (IOException e) {
            System.out.println("io exception");
        }

        if (count > 0) {
            collection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("Time taken to write using %s compression: %d",
                this.compressionType, (end - start)));
    }

    public void shutdownServer() {
        this.client.close();
    }

    public static void main(String[] args) {
        Map<String, String> typeToConnectionUri = new HashMap<>() {{
            /*put("zstd", "mongodb://localhost:27017/?compressors=zstd");*/
            put("zlib", "mongodb://localhost:27017/?compressors=zlib");
            put("snappy", "mongodb://localhost:27017/?compressors=snappy");
        }};
        SyncDriverBenchmark sdb;

        for (Map.Entry<String, String> entry : typeToConnectionUri.entrySet()) {
            sdb = new SyncDriverBenchmark(entry.getValue(), entry.getKey());
            sdb.getAllDocs("test", "trades");
            sdb.write("write", "trades", "/home/ramasai/trades.json");
            sdb.shutdownServer();
        }
    }
}
