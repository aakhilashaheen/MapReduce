import java.lang.*;
import java.io.*;
import java.util.*;
import java.time.*;
import java.lang.Thread;

public class MapTask extends Thread {

    private HashSet<String> positives, negatives;
    private String inputFile, outputFile;
    public MapTask(String iF, HashSet<String> p, HashSet<String> n) {
        this.positives = p;
        this.negatives = n;
        this.inputFile = iF;
    }

    @Override
    public void run() {
        countSentiment();
    }

    public void countSentiment() {
        int sentiment = 0;
        try {
            FileReader fr = new FileReader(this.inputFile);
            BufferedReader br = new BufferedReader(fr);

            System.out.println("Analyzing file" + this.inputFile);
            String line = null;
            while((line = br.readLine()) != null) {
                String[] words = line.split("\\W");
                for(int i = 0; i < words.length; ++i) {
                    String word = words[i].toUpperCase();
                    if(positives.contains(word))
                        ++sentiment;
                    else if(negatives.contains(word))
                        --sentiment;
                }
            }
            br.close();
        } catch (Exception e) { }

        this.outputFile = this.inputFile.substring(this.inputFile.lastIndexOf("/") + 1)
                + "_" + Instant.now().toString() + ".txt";
        System.out.println("I'm going to write to " + this.outputFile);
        try {
            FileWriter fw = new FileWriter("intermediate_dir/" + this.outputFile);
            BufferedWriter bw = new BufferedWriter(fw);
            System.out.println("Storing " + this.inputFile + " analysis in " + this.outputFile);
            bw.write(this.inputFile.substring(this.inputFile.lastIndexOf("/") + 1)
                    + "," + Integer.toString(sentiment));
            bw.close();
        } catch (Exception e) { }
    }

    public String getOutputFile() {
        return this.outputFile;
    }
}