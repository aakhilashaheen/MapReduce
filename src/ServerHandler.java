import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerHandler implements ServerService.Iface{
    private static int serverPort;
    private static int schedulingPolicy;
    private static final String int_dir = "intermediate_dir/"; //Intermediate Folder
    private static final String out_dir = "output_dir/";
    Machine self;
    List<Machine> computeNodes = new ArrayList<>();
    ConcurrentLinkedQueue<String> inputFiles = new ConcurrentLinkedQueue<>();
    @Override
    public int enroll(Machine machine) throws TException {
       computeNodes.add(machine);
	System.out.println("Succesfully registered the machine"+machine.ipAddress);
	return schedulingPolicy;
    }

    @Override
    public String mapReduceJob(List<String> inputfileNames) throws TException {

        System.out.println("Received list to mapreduce");
        inputfileNames.forEach(System.out :: println);
        for(String fileName : inputfileNames){
            inputFiles.add(fileName);
        }

        submitMapJobsToComputeNodes();

        //Split the list of files into multiple tasks and send them to the worker nodes
        return "output_dir";
    }

    public void submitMapJobsToComputeNodes(){
        ExecutorService executor = Executors.newFixedThreadPool(5);


        while(!inputFiles.isEmpty()){
            Runnable worker = new MapTaskSender(computeNodes,inputFiles);
            executor.execute(worker);
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
    }

    public ServerHandler(Integer port) throws Exception {
       /* this.inProgress = new HashMap<>();
        this.tasks = new ConcurrentLinkedQueue<>();
        this.completed = new ConcurrentLinkedQueue<>();

        this.i_complete = 0;
        this.i_unique = 0L;*/

        //Create a Machine data type representing ourselves
        self = new Machine();
        self.ipAddress = InetAddress.getLocalHost().getHostName();
        self.port = port;

        //initialize folder(s)
        if(!(new File(int_dir)).mkdir()) //one line folder init mkdir bby!
            System.out.println("Folder already exists: " + int_dir);
        if(!(new File(out_dir)).mkdir())
            System.out.println("Folder already exists: " + out_dir);
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
