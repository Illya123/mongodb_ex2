import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.time.DateUtils;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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

              // Date tsToDay = DateUtils.truncate(new Date((long)currentWord.getTimestamp()), Calendar.DATE);
              //   Date tsToDay = new Date(Instant.ofEpochSecond(currentWord.getTimestamp()).truncatedTo(ChronoUnit.DAYS).toEpochMilli());
                Date tsToDay = new Date (toDay(Objects.toString(currentWord.getTimestamp())));
                if (!removeDups(doc.get(currentWord.getWord()), tsToDay, currentWord.getFrequency(), currentWord.getWord()))
                {
                    Document subDoc = new Document()
                            .append("ts", tsToDay)
                            .append("freq", currentWord.getFrequency());
                    doc.get(currentWord.getWord()).add(subDoc);
                }

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
        basicBSONs.add(new Document("$match", new Document("values.ts", new Document("$lte", to)
                                .append("$gte", from))));

        basicBSONs.add(new Document("$group", new Document("_id", null)
                                .append("values", new Document("$push", new Document("freq", "$values.freq")
                                .append("ts", "$values.ts")))));
        basicBSONs.add(new Document("$project", new Document("values", true)
                                .append("_id", false)));

        return basicBSONs;
    }

    private boolean removeDups(List<Document> docsWithDups, Date timestamp, int freq, String word){

        for (Document d : docsWithDups){
            //System.out.println(word);
            Object o = d.get("ts");
            if((o instanceof Integer)){
                System.out.println(word + ": " + o.toString());
                continue;
            }
            Date date = d.getDate("ts");
            if((date.compareTo(timestamp)) == 0){
                int newFreq = freq + d.getInteger("freq");
                d.put("ts", newFreq);
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
