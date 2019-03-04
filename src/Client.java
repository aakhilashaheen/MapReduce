import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Client {
    Scanner sc;

    TTransport serverTransport;
    ServerService.Client server;

    final static String defaultDir = "./data/"; //default data directory

    //Connect to the superNode
    public Client(Machine serverInfo) throws TException {
        sc = new Scanner(System.in);
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
            System.err.println("Usage: java Client <server> <port>");
            return;
        }
        try {
            Machine serverInfo = new Machine();
            serverInfo.ipAddress = args[0];
            serverInfo.port = Integer.parseInt(args[1]);

            Client client = new Client(serverInfo);

            while(!client.connectToServer()) {
                System.err.println("Client: Failed to connect to a server on cluster, retrying in 1 second ...");
                Thread.sleep(1000);
            }

            System.out.println("Contacted server at " + serverInfo.ipAddress + ":" + serverInfo.port);
            System.out.println("\n\n -------- Welcome to the Terminal for Map Reduce --------\n\n");
            Scanner scan = new Scanner(System.in);
            System.out.println("Enter the number of files to process");

            int n = scan.nextInt();
            List<String> inputFiles = new ArrayList<>();
            for( int i = 0; i < n ; i++){
                inputFiles.add("input_dir/" + scan.next());
            }
            client.submitJob(inputFiles);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }



    /* From the assignment description the job server will receive only one job at a time
    */
    private boolean submitJob(List<String> inputFiles) throws TException {
        List<String> legitFiles = new ArrayList<>();
        //check for existence of file
        for(String inputFile : inputFiles){
            File file = new File(inputFile);
            if(!file.exists() || file.isDirectory()) {
                System.out.println("Not a file or is a directory" + file.getName());
            } else {
                legitFiles.add(inputFile);
            }
        }

        String result = server.mapReduceJob(legitFiles);

        if(result.equals("NULL")) {
            System.out.println("Job failed");
            return false;
        } else {
            System.out.println("Output file located at :\n    " + result); //just the filename, no paths allowed
            return true;
        }
    }
}
