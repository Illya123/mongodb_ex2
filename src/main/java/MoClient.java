import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BasicBSONObject;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.*;
import java.util.*;

/**
 * Created by illya on 12.11.17.
 */
public class MoClient
{
    private String user = "mongoUser";
    private String database = "infsys";
    private char [] password = "asdf123".toCharArray();

    private String colName = "words";

    private MongoCredential credential = MongoCredential.createCredential(user, database, password);
    private ServerAddress serverAddress = new ServerAddress("141.28.68.212", 27017);
    private MongoClient client = new MongoClient( serverAddress, Collections.singletonList(credential));

    private MongoDatabase db = client.getDatabase("infsys");

    public MoClient()
    {

    }
    public MongoDatabase getDb()
    {
        return db;
    }

    public void setDb(MongoDatabase db)
    {
        this.db = db;
    }

    public void insertData(){
        try
        {
            HashMap<String, List<Document>> doc = new HashMap<>();
            ArrayList<Document> docs = new ArrayList<>();

            for (String line : Files.readAllLines(Paths.get("./words.txt")))
            {
                Word currentWord = new Word(line);
                if(!doc.containsKey(currentWord.getWord()))
                {
                    doc.put(currentWord.getWord(), new ArrayList<>());
                }
                Document subDoc = new Document()
                        .append("ts", new Date(currentWord.getTimestamp()))
                        .append("freq", currentWord.getFrequency());
                doc.get(currentWord.getWord()).add(subDoc);
            }

            for( String key : doc.keySet()){
                Document document = new Document()
                        .append("name", key)
                        .append("values", doc.get(key));
                docs.add(document);
            }

            db.getCollection(this.colName).insertMany(docs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public Map<Date,Integer> query(String word, Date from, Date to){
        HashMap<Date, Integer> queriedData = new HashMap<>();
        AggregateIterable<Document> agrIt = this.db.getCollection(this.colName)
                                                .aggregate(this.aggregate(word, from, to));
        for (Document doc : agrIt)
        {
            for (Document d : (ArrayList<Document>) doc.get("values"))
            {
                queriedData.put((Date) d.get("ts"), (int) d.get("freq"));
            }
        }
        return queriedData;
    }

    private List<Document> aggregate(String word, Date from, Date to){
        List<Document> basicBSONs = new ArrayList<>();

        basicBSONs.add(new Document("$match", new Document("name", word)));
        basicBSONs.add(new Document("$unwind", "$values"));
        basicBSONs.add(new Document("$match", new Document("values.ts", new Document("$gte", from)
                                .append("$lte", to))));

        basicBSONs.add(new Document("$group", new Document("_id", null)
                                .append("values", new Document("$push", new Document("freq", "$values.freq")
                                .append("ts", "$values.ts")))));
        basicBSONs.add(new Document("$project", new Document("values", true)
                                .append("_id", false)));

        return basicBSONs;
    }
}
