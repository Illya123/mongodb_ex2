import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;

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
    //141.28.68.212
    private ServerAddress serverAddress = new ServerAddress("192.168.1.14", 27017);
    private MongoClient client = new MongoClient( serverAddress, Collections.singletonList(credential));

    private MongoDatabase db = client.getDatabase("infsys");

    MoClient()
    {

    }
    MongoDatabase getDb()
    {
        return db;
    }

    public void setDb(MongoDatabase db)
    {
        this.db = db;
    }

    void insertDataInsertMany(){
        try
        {
            HashMap<String, List<Document>> docsMap = new HashMap<>();
            ArrayList<Document> docs = new ArrayList<>();

            for (String line : Files.readAllLines(Paths.get("./words.txt")))
            {
                Word currentWord = new Word(line);

                if(!docsMap.containsKey(currentWord.getWord()))
                {
                    docsMap.put(currentWord.getWord(), new ArrayList<>());
                }

                 Date tsToDay = new Date (toDay(Objects.toString(currentWord.getTimestamp())));
                if (!removeDups(docsMap.get(currentWord.getWord()), tsToDay, currentWord.getFrequency(), currentWord.getWord()))
                {
                    Document subDoc = new Document()
                            .append("ts", tsToDay)
                            .append("freq", currentWord.getFrequency());
                    docsMap.get(currentWord.getWord()).add(subDoc);
                }

            }

            for( String key : docsMap.keySet()){
                Document document = new Document()
                        .append("name", key)
                        .append("values", docsMap.get(key));
                docs.add(document);
            }

            db.getCollection(this.colName).insertMany(docs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    void insertDataUnefficientAsFuck(){
        try
        {
            HashMap<String, List<Document>> docsMap = new HashMap<>();
            ArrayList<Document> docs = new ArrayList<>();

            for (String line : Files.readAllLines(Paths.get("./words_short.txt")))
            {
                Word currentWord = new Word(line);
                MongoCollection<Document> col = this.db.getCollection(this.colName);
                FindIterable<Document> document = col.find(eq("name", currentWord.getWord()));
                Document newDocument;

                if (document.first() != null)
                {
                    List<Document> tsDocs = (List<Document>) document.first().get("values");
                    for (Document doc : tsDocs)
                    {
                        Date currentTs = doc.getDate("ts");
                        int currentFreq = doc.getInteger("freq");
                        List<Document> tsList;


                        if(currentTs.compareTo(new Date(currentWord.getTimestamp())) != 0)
                        {
                            //CurrentTimestamp does not exist in the DB.
                            newDocument = new Document("name", currentWord.getWord());
                            tsList = tsDocs;
                            newDocument.append("values", tsList);

                            tsList.add(new Document("ts", new Date(currentWord.getTimestamp()))
                                    .append("freq", currentWord.getFrequency()));
                        }
                        else
                        {
                            //CurrentTimestamp does exist in the DB.
                            doc.put("freq", currentFreq + currentWord.getFrequency());

                            newDocument = new Document("name", currentWord.getWord());
                            tsList = tsDocs;
                            newDocument.append("values", tsList);
                        }
                        col.replaceOne(new Document("name", currentWord.getWord()), newDocument, new UpdateOptions().upsert( true ));
                        break;
                    }
                }
                else
                {
                    //Create Document if it does not exist.
                    Document doc = new Document("name", currentWord.getWord());
                    List<Document> tsList = new ArrayList<>();
                    doc.append("values", tsList);

                    tsList.add(new Document("ts", new Date(currentWord.getTimestamp()))
                            .append("freq", currentWord.getFrequency()));
                    col.insertOne(doc);
                }
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    Map<Date,Integer> query(String word, Date from, Date to){
        HashMap<Date, Integer> queriedData = new HashMap<>();
        AggregateIterable<Document> agrIt = this.db.getCollection(this.colName)
                                                .aggregate(this.aggregate(word, from, to));
        for (Document doc : agrIt)
        {
            for (Document d : (ArrayList<Document>) doc.get("values"))
            {
                queriedData.put( d.getDate("ts"), d.getInteger("freq"));
            }
        }
        return queriedData;
    }

    private List<Document> aggregate(String word, Date from, Date to){
        List<Document> basicBSONs = new ArrayList<>();

        basicBSONs.add(new Document("$match", new Document("name", word)));
        basicBSONs.add(new Document("$unwind", "$values"));
        basicBSONs.add(new Document("$match", new Document("values.ts", new Document("$lte", to)
                                .append("$gte", from))));

        basicBSONs.add(new Document("$group", new Document("_id", null)
                                .append("values", new Document("$push", new Document("freq", "$values.freq")
                                .append("ts", "$values.ts")))));
        basicBSONs.add(new Document("$project", new Document("_id", false)
                                .append("values", true)));

        return basicBSONs;
    }

    private boolean removeDups(List<Document> docsWithDups, Date timestamp, int freq, String word){

        for (Document d : docsWithDups){
            Date date = d.getDate("ts");
            if((date.compareTo(timestamp)) == 0){
                int newFreq = freq + d.getInteger("freq");
                d.put("freq", newFreq);
                return true;
            }
        }
        return false;
    }

    private static long toDay(String timestamp) {

        StringBuilder timestampBuilder = new StringBuilder(timestamp);
        while (timestampBuilder.length() < 13) timestampBuilder.append("0");
        timestamp = timestampBuilder.toString();
        long ts = Long.parseLong(timestamp);
        return ts - ts % 86400000;
    }
}