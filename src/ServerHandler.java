import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerHandler implements ServerService.Iface{
    private static int serverPort;
    private static int schedulingPolicy;
    private static final String inputDirectoryPath = "input_dir/";
    private static final String intermediateDirectoryPath = "intermediate_dir";
    private static final String outputDirectoryPath = "output_dir";
    private int countOfMapJobsPerInput = 0;
    private AtomicInteger countOfCompletedMapJobsPerInput = new AtomicInteger(0);
    private ExecutorService executor = Executors.newFixedThreadPool(10);
    private String intermediateDirectoryForJobInProcess = "";
    private String outFileForJob = "";
    Node self;
    List<Node> computeNodes = new ArrayList<>();
    boolean jobInProgress ;


    @Override
    public int enroll(Node node) throws TException {
       computeNodes.add(node);
	System.out.println("Succesfully registered the node"+node.ipAddress);
	return schedulingPolicy;
    }

    @Override
    public String mapReduceJob(String inputDirectorytoBeProcessed) throws TException {
        jobInProgress = true;
        File inputDirectory = new File(inputDirectorytoBeProcessed);
        System.out.println("Received inputput directory for sentiment analysis "+ inputDirectory.getName());
        countOfMapJobsPerInput = inputDirectory.listFiles().length;
        for(File inputFile : inputDirectory.listFiles()){
            Runnable worker = new MapTaskSender(computeNodes,inputFile.getName());
            System.out.println(inputFile.getName());
            executor.execute(worker);
        }

        while(!jobInProgress){

        }
        return outFileForJob ;
    }

    //If the count of the mapTasksToBeProcessed is equal to the MapJobsFinished, start the sort task
    @Override
    public void completedMapTask(String inputFile, String intermediateDirectory) throws TException {
        System.out.println("REceived completed map task call");
        System.out.println(countOfCompletedMapJobsPerInput);
        if(countOfCompletedMapJobsPerInput.incrementAndGet() == countOfMapJobsPerInput) {

            intermediateDirectoryForJobInProcess = intermediateDirectory;
            submitSortJobToComputeNode();
        }
    }

    //If the job has finished, this is called by the worker nodes
    @Override
    public void completedSortTask(String outputFile) throws TException {
        outFileForJob = outputFile;
      jobInProgress = false;
    }

    /* Randomly assigns the sort job to one of the worker Nodes

     */
    public void submitSortJobToComputeNode(){
        Node m = computeNodes.get(0);
        SortTaskSender sortTaskSender = new SortTaskSender(m, intermediateDirectoryForJobInProcess);
        sortTaskSender.run();
    }


    public ServerHandler(Integer port) throws Exception {

        //Create a Machine data type representing ourselves
        self = new Node();
        self.ipAddress = InetAddress.getLocalHost().getHostName();
        self.port = port;

        //initialize folder(s)
        if(!(new File(intermediateDirectoryPath)).mkdir()) //one line folder init mkdir bby!
            System.out.println("Folder already exists: " + intermediateDirectoryPath);
        if(!(new File(outputDirectoryPath)).mkdir())
            System.out.println("Folder already exists: " + outputDirectoryPath);
    }
    public static void main(String [] args){
        // Inputs should be the port of the server, scheduling policy as 0/1
        if(args.length < 2){
            System.out.println("Please enter the port and the scheduling policy");
            return;
        }

        serverPort = Integer.parseInt(args[0]);
        schedulingPolicy = Integer.parseInt(args[1]);
        try{
            //Thrift code for starting the server handler
            ServerHandler server = new ServerHandler(serverPort);
            server.start();

        }catch(Exception e){
            System.out.println("Could not start the server");
        }

    }



    //Begin Thrift Server instance for a Node and listen for connections on our port
    private void start() throws TException {


        //Create Thrift server socket
        TServerTransport serverTransport = new TServerSocket(self.port);
        TTransportFactory factory = new TFramedTransport.Factory();
        ServerService.Processor processor = new ServerService.Processor<>(this);

        //Set Server Arguments
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor); //Set handler
        serverArgs.transportFactory(factory); //Set FramedTransport (for performance)

        //Run server with multiple threads
        TServer server = new TThreadPoolServer(serverArgs);

        System.out.println("Server is listening ... ");
        server.serve();
    }

}
