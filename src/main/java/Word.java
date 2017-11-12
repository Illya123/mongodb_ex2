import java.util.*;

/**
 * Created by illya on 10.10.17.
 */
public class Word
{
    private String word;
    private int frequency;
    private long timestamp;

    public Word(String line)
    {
        List<String> data = Arrays.asList(line.split("  "));
        this.word = data.get(0);
        this.frequency = Integer.parseInt(data.get(1));
        this.timestamp = Long.parseLong(data.get(2));
    }

    public String getWord()
    {
        return word;
    }

    public void setWord(String word)
    {
        this.word = word;
    }

    public int getFrequency()
    {
        return frequency;
    }

    public void setFrequency(int frequency)
    {
        this.frequency = frequency;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

}
