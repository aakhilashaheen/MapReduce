import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerHandler implements WorkerNodeService.Iface{

    Node self;
    Node server;
    double loadProbability = 0.0;
    int protocol = 0 ; //Default 0 : random scheduling protocol, 1: load balancing protocol
    ConcurrentLinkedQueue<String> taskQueue;
    AtomicInteger mapTasksReceived =  new AtomicInteger(0);
    AtomicInteger mapTasksRejected = new AtomicInteger(0);
    AtomicInteger mapTasksProcessd = new AtomicInteger(0);
    @Override
    public boolean mapTask(String inputFilename) throws TException {
        //Received the task for mapping
        System.out.println("Worker received the file for mapping "+inputFilename);
        if(shouldRejectTheTask()){
            System.out.println("Map tasks rejected " + mapTasksRejected.incrementAndGet());
            return false;
        }
        System.out.println("Map tasks received " + mapTasksReceived.incrementAndGet());
        synchronized (taskQueue){
            taskQueue.add(inputFilename);
            System.out.println("Map tasks processed " + mapTasksProcessd.incrementAndGet());

        }
        return true;
    }

    public boolean shouldRejectTheTask(){
        Random rand = new Random();
        return(rand.nextDouble() > loadProbability ? false : true);
    }

    @Override
    public String sortTask(String intermediateFilesFolder) throws TException {
        //Received the task for mapping
        //if(shouldRejectTheTask())
        //    return "";
        SortTask task = new SortTask(intermediateFilesFolder, server);
        task.sortFiles();
        return task.getOutputFile();
    }

    /* Constructor for a Server, a Thrift connection is made to the server as well */
    public WorkerHandler(String serverIP, Integer serverPort, Integer port, double loadProbability) throws Exception {
        // connect to the server as a client
        TTransport serverTransport = new TSocket(serverIP, serverPort);
        serverTransport.open();
        TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
        ServerService.Client serverClient = new ServerService.Client(serverProtocol);

        this.server = new Node();
        this.server.ipAddress = serverIP;
        this.server.port = serverPort;

        //Create a Machine data type representing ourselves
        self = new Node();
        self.ipAddress = InetAddress.getLocalHost().getHostName().toString();
        self.port = port;

        taskQueue = new ConcurrentLinkedQueue<>();
        this.loadProbability = loadProbability;
        WorkerTaskQueueHandler watcher = new WorkerTaskQueueHandler(this,taskQueue, server);
        watcher.start();
        // call enroll on superNode to enroll.
        protocol = serverClient.enroll(self);
        System.out.println("Worker node protocol received from server");

        serverTransport.close();
    }
    //Begin Thrift Server instance for a Node and listen for connections on our port
    private void start() throws TException {

        //Create Thrift server socket
        TServerTransport serverTransport = new TServerSocket(self.port);
        TTransportFactory factory = new TFramedTransport.Factory();

        WorkerNodeService.Processor processor = new WorkerNodeService.Processor<>(this);

        //Set Server Arguments
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor); //Set handler
        serverArgs.transportFactory(factory); //Set FramedTransport (for performance)

        //Run server with multiple threads
        TServer server = new TThreadPoolServer(serverArgs);

        server.serve();
    }
    public static void main(String[] args) {
        if(args.length < 3) {
            System.err.println("Usage: java ComputeNodeHandler <serverIP> <serverPort> <port> <loadProbability>");
            return;
        }
        try {
            System.out.println("IP Address is " + InetAddress.getLocalHost().toString());
            String serverIP = args[0];
            Integer serverPort = Integer.parseInt(args[1]);
            Double loadProbability = new Double(args[3]);
            //port number used by this node.
            Integer port = Integer.parseInt(args[2]);

            WorkerHandler server = new WorkerHandler(serverIP, serverPort,port, loadProbability);

            //spin up server
            server.start();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
