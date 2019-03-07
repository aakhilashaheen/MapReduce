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
import java.util.concurrent.atomic.AtomicLong;

public class MapTaskHandler extends Thread {

    private HashSet<String> positives, negatives;
    private String inputFile, outputFile;
    private static String input_dir = "input_dir/";
    private static String intermediate_dir = "intermediate_dir/";
    private static String logging_dir = "logging_dir/";
    private static String inputFileReceived = "";
    private static Node server;
    private Node worker;
    private double loadProbability;
    MapTaskStatistics mapTaskStatistics;
    public MapTaskHandler(String iF, HashSet<String> p, HashSet<String> n, Node server, Node worker, double loadProbability,MapTaskStatistics mapTaskStatistics) {
        this.positives = p;
        this.negatives = n;
        this.inputFile = input_dir+iF;
        this.server = server;
        this.worker = worker;
        inputFileReceived = iF;
        this.loadProbability = loadProbability;
        this.mapTaskStatistics = mapTaskStatistics;
    }

    @Override
    public void run() {
        countSentiment();
    }

    public void countSentiment() {
        int wordCount = 0;
        double pSentiment = 0, nSentiment = 0;
        long startTime = System.currentTimeMillis();
        System.out.println("Start time for : "+inputFile+startTime);
        injectDelay();
        try {
            FileReader fr = new FileReader(this.inputFile);
            BufferedReader br = new BufferedReader(fr);

            String line = null;
            while((line = br.readLine()) != null) {
                String[] words = line.replaceAll("--"," ").split("[^a-zA-Z\\-']+");
                for(int i = 0; i < words.length; ++i) {
                    String word = words[i].toLowerCase().replaceAll("[^a-zA-Z\\-']","").trim();
                    if(word.contains("'")) {
                        word = word.substring(0, word.indexOf("'"));
                    }
                    word = word.trim();
                    //System.out.println(word);
                    wordCount++;
                    if(!word.equals("") && positives.contains(word))
                        pSentiment++;
                    else if(!word.equals("") && negatives.contains(word))
                        nSentiment++;
                }
            }
            br.close();
        } catch (Exception e) { e.printStackTrace(); }
        long endTime = System.currentTimeMillis();
        System.out.println("End time for : "+inputFile+endTime);
        this.outputFile = this.inputFile.substring(this.inputFile.lastIndexOf("/") + 1)
	    + "_" + Instant.now().toString() + ".txt";
        try {
            FileWriter fw = new FileWriter(this.intermediate_dir + this.outputFile);
            BufferedWriter bw = new BufferedWriter(fw);
            double sentiment = (pSentiment - nSentiment)/(pSentiment + nSentiment);
            String output = this.inputFile.substring(this.inputFile.lastIndexOf("/") + 1)
                    + "," + Double.toString(sentiment);
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
            e.printStackTrace();
        }

        System.out.println("Total number of Map tasks processed by this worker : " + mapTaskStatistics.incrementAndGetCounts());
        System.out.println("Total time taken for Map tasks by this worker so long: " + mapTaskStatistics.incrementAndGetTimeTakenForMapTasks(endTime-startTime));

    }

    public String getOutputFile() {
        return this.outputFile;
    }

    private void injectDelay(){
        double roll = new Random().nextDouble();
        if(roll < this.loadProbability){
            try {
                Thread.sleep(3000);
                System.out.println("Injecting delay");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
