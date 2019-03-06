import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
/* A client program that when run connects to the server running at the given port. This submits the input directory for sentiment analysis and outputs
an output directory which contains the ordered list of files based on their sentiment scores.
 */
public class Client {
    TTransport serverTransport;
    ServerService.Client server;
    //Connect to the Server
    public Client(Node serverInfo) throws TException {
        serverTransport = new TSocket(serverInfo.ipAddress, serverInfo.port);
    }

    private boolean connectToServer() {
        try {
            serverTransport.open();
            TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
            server = new ServerService.Client(serverProtocol);
            return true;
        }
        catch(TException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
        if(args.length < 2) {
            System.err.println("Usage: java Client <serverIp> <serverPort>");
            return;
        }
        try {
            Node serverInfo = new Node();
            serverInfo.ipAddress = args[0];
            serverInfo.port = Integer.parseInt(args[1]);

            Client client = new Client(serverInfo);

            while(!client.connectToServer()) {
                System.err.println("Client: Failed to connect to a server on cluster, retrying in 1 second ...");
                Thread.sleep(1000);
            }

            System.out.println("Contacted server at " + serverInfo.ipAddress + ":" + serverInfo.port);
            System.out.println("\n\n -------- Welcome to the Terminal for Sentiment Analysis--------\n\n");
            String directoryToBeProcessed = args[2];

            File inputDirectory = new File(directoryToBeProcessed);

            if(!inputDirectory.isDirectory()){
                System.out.println("No input directory exists at " +directoryToBeProcessed);
                return;
            }

            client.submitJob(directoryToBeProcessed);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }



    /* From the assignment description the job server will receive only one job at a time
    */
    private boolean submitJob(String directoryToBeProcessed) throws TException {


        String result = server.mapReduceJob(directoryToBeProcessed);

        if(result.equals("")) {
            System.out.println("Job failed");
            return false;
        } else {
            System.out.println("Output file located at :\n    " + result); //just the filename, no paths allowed
            return true;
        }
    }
}
