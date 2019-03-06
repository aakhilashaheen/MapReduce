import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.*;
import java.nio.Buffer;
import java.time.Instant;
import java.util.*;

public class SortTask extends Thread{
    private String intermediateDirectory;
    private String outputFile;
    private Node server;

    public SortTask(String intermediateDirectory, Node server) {
        this.intermediateDirectory = intermediateDirectory;
        this.server = server;
    }

    @Override
    public void run() {
        sortFiles();
    }

    public void sortFiles() {
        File[] listOfFiles = (new File("intermediate_dir")).listFiles();
        System.out.println("Number of intermediate files is"+listOfFiles.length);
        for(int i = 0 ; i < listOfFiles.length;i++){
            System.out.println(listOfFiles[i]);
        }
        ArrayList<String> intermediateFiles = new ArrayList<>(listOfFiles.length);
        LinkedList<Pair<String, Double>> files = new LinkedList<Pair<String, Double>>();
        for(File intermediateFile : listOfFiles) {
            try {

                FileReader fr = new FileReader(intermediateFile);
                BufferedReader br = new BufferedReader(fr);
                //System.out.println("Reading file " + filename);
                String line = br.readLine();
                int idx = line.indexOf(",");
                Pair<String, Double> pair = new Pair<>(line.substring(0, idx), Double.parseDouble(line.substring(idx + 1)));
                files.add(pair);
                //System.out.println(pair.toString());
            } catch (Exception e) { }
        }
        Collections.sort(files, (p1, p2) -> (int) Math.signum(p2.second - p1.second));
        this.outputFile = "output_dir/" + Instant.now().toString() + ".txt";
        try {
            FileWriter fw = new FileWriter(this.outputFile);
            BufferedWriter bw = new BufferedWriter(fw);
            for(Pair<String, Double> pair : files) {
                System.out.println(pair.toString());
                bw.write(pair.first + "\n");
            }
            bw.close();
            //Send the update to the server
            TTransport serverTransport = new TSocket(server.ipAddress, server.port);
            serverTransport.open();
            TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
            ServerService.Client server = new ServerService.Client(serverProtocol);
            server.completedSortTask(outputFile);
            serverTransport.close();
        } catch (Exception e) { }
    }

    public String getOutputFile() {
        return this.outputFile;
    }
}