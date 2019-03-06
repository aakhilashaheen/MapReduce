import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.lang.*;
import java.io.*;
import java.util.*;
import java.time.*;
import java.lang.Thread;

public class MapTask extends Thread {

    private HashSet<String> positives, negatives;
    private String inputFile, outputFile;
    private static String input_dir = "input_dir/";
    private static String intermediate_dir = "intermediate_dir/";
    private static String inputFileReceived = "";
    private static Node server;
    public MapTask(String iF, HashSet<String> p, HashSet<String> n,Node server) {
        this.positives = p;
        this.negatives = n;
        this.inputFile = input_dir+iF;
        this.server = server;
        inputFileReceived = iF;
    }

    @Override
    public void run() {
        countSentiment();
    }

    public void countSentiment() {
        double pSentiment = 0, nSentiment = 0;
        try {
            FileReader fr = new FileReader(this.inputFile);
            BufferedReader br = new BufferedReader(fr);
            System.out.println("Hello I'm the map task");
            System.out.println("Analyzing file " + this.inputFile);
            String line = null;
            while((line = br.readLine()) != null) {
                String[] words = line.replaceAll("--"," ").split("[^a-zA-Z\\-']+");
                for(int i = 0; i < words.length; ++i) {
                    String word = words[i].toLowerCase().replaceAll("[^a-zA-Z\\-']","").trim();
                    if(word.contains("'")) {
                        word = word.substring(0, word.indexOf("'"));
                    }
                    word.trim();
                    //System.out.println(word);

                    if(!word.equals("") && positives.contains(word))
                        pSentiment++;
                    else if(!word.equals("") && negatives.contains(word))
                        nSentiment++;
                }
            }
            br.close();
        } catch (Exception e) { e.printStackTrace(); }

        this.outputFile = this.inputFile.substring(this.inputFile.lastIndexOf("/") + 1)
	    + "_" + Instant.now().toString() + ".txt";
        System.out.println("I'm going to write to " + this.outputFile);
        try {
            FileWriter fw = new FileWriter("intermediate_dir/" + this.outputFile);
            BufferedWriter bw = new BufferedWriter(fw);
            //System.out.println("Storing " + this.inputFile + " analysis in " + this.outputFile);
            System.out.println("Positive: " + Double.toString(pSentiment) +
                    ", Negative: " + Double.toString(nSentiment) + ", Total: " + Double.toString(pSentiment + nSentiment));
            double sentiment = (pSentiment - nSentiment)/(pSentiment + nSentiment);
            System.out.println("This is the sentiment!!!!!!!" +sentiment);
            String output = this.inputFile.substring(this.inputFile.lastIndexOf("/") + 1)
                    + "," + Double.toString(sentiment);
            System.out.println("Output: " + output);
            bw.write(output);
            bw.close();
            //Send the update to the server
            TTransport serverTransport = new TSocket(server.ipAddress, server.port);
            serverTransport.open();
            TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
            ServerService.Client server = new ServerService.Client(serverProtocol);
            server.completedMapTask(inputFileReceived, outputFile);
            serverTransport.close();
        } catch (Exception e) {

        }
    }

    public String getOutputFile() {
        return this.outputFile;
    }

}
