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
import java.util.concurrent.atomic.AtomicInteger;

public class MapTask extends Thread {

    private HashSet<String> positives, negatives;
    private String inputFile, outputFile;
    private static String input_dir = "input_dir/";
    private static String intermediate_dir = "intermediate_dir/";
    private static String inputFileReceived = "";
    private AtomicInteger timeTakenToProcess;
    private static Node server;
    private double loadProbability;
    public MapTask(String iF, HashSet<String> p, HashSet<String> n, Node server, double loadProbability, AtomicInteger timeTakenToProcessRequest) {
        this.positives = p;
        this.negatives = n;
        this.inputFile = input_dir+iF;
        this.server = server;
        inputFileReceived = iF;
        this.timeTakenToProcess = timeTakenToProcessRequest;
        this.loadProbability = loadProbability;
    }

    @Override
    public void run() {
        countSentiment();
    }

    public void countSentiment() {
        double pSentiment = 0, nSentiment = 0;
        long startTime = System.currentTimeMillis();
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
        try {
            FileWriter fw = new FileWriter("intermediate_dir/" + this.outputFile);
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

        }
        long endTime = System.currentTimeMillis();
        this.timeTakenToProcess.addAndGet((int) (endTime-startTime));
    }

    public String getOutputFile() {
        return this.outputFile;
    }

    private void injectDelay(){
        double roll = new Random().nextDouble();
        if(roll < this.loadProbability){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
