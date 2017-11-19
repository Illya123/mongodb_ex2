/**
 * Created by illya on 12.11.17.
 */

import com.mongodb.*;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;

public class Main
{

    public static void main(String[] args)
    {
        String searchWord = "hello";
        Date from = new Date(0000000000000L);
        Date to = new Date(2434827180000L);
        int wordFreq = 0;

        MoClient mongoClient = new MoClient();
        mongoClient.getDb().drop();

        long startTimeIns = System.nanoTime();
        //exercise3
        mongoClient.insertDataInsertMany();
        //exercise2
       // mongoClient.insertDataUnefficientAsFuck();
        long endTimeIns = System.nanoTime();

        System.out.println("The query needed " + ((endTimeIns - startTimeIns)/1000000) + " ms");

        // Query time
        long startTimeQ = System.nanoTime();
        Map<Date, Integer> queriedData = mongoClient.query(searchWord, from, to);
        long endTimeQ = System.nanoTime();

        System.out.println("The query needed " + ((endTimeQ - startTimeQ)/1000000) + " ms");

        // Calculate the word frequency by adding all frequencies
        printFreq(searchWord, calcWordFreq(queriedData), from, to, queriedData.size());
    }



    /**
     * Returns the frequency of the word
     * @param querieData
     * @return
     */
    public static int calcWordFreq(Map<Date, Integer> querieData){
        int wordSum = 0;
        for(Map.Entry<Date, Integer> entry : querieData.entrySet()){
            wordSum += entry.getValue();
        }

        return wordSum;
    }

    public static void printFreq(String searchedWord, int freq, Date from, Date to, int length){
        DateFormat gerFormat = new SimpleDateFormat( "dd.MM.yy hh:mm:ss");

        System.out.println("In the Inverval: " + gerFormat.format(from) + " - " + gerFormat.format(to) + ", " +
                "\""+searchedWord +"\"" + " appeared " + length + " times and its frequency is " + freq);
    }
}
