import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
/*The entry point for worker node which handles map and sort tasks

 */
public class WorkerHandler implements WorkerNodeService.Iface{

    Node self;
    Node server;
    double loadProbability = 0.0;
    int protocol = 0 ; //Default 0 : random scheduling protocol, 1: load balancing protocol
    ConcurrentLinkedQueue<String> taskQueue;
    AtomicLong mapTasksReceived =  new AtomicLong(0);
    AtomicLong mapTasksRejected = new AtomicLong(0);
    AtomicInteger mapTasksProcessd = new AtomicInteger(0);
    AtomicLong timeTakenToMap = new AtomicLong(0);
    AtomicLong timeTakenToSort = new AtomicLong(0);
    MapTaskStatistics mapTaskStatistics;
    /*This takes the file for mapping and either processes it or rejects it.
    If being processed, it places it into a worker queue for processing
     */
    @Override
    public boolean mapTask(String inputFilename) throws TException {
        //Received the task for mapping
        System.out.println("Total number of Map tasks rejected by this worker : " + mapTasksRejected);
        if(shouldRejectTheTask()){
            mapTasksRejected.incrementAndGet();
            return false;
        }
        synchronized (taskQueue){
            taskQueue.add(inputFilename);

        }
        return true;
    }

    /* Rejects the task on the basis of scheduling protocol and loadProbability*/
    public boolean shouldRejectTheTask(){
        if(protocol == 1) {
            Random rand = new Random();
            return (rand.nextDouble() > loadProbability ? false : true);
        }
        return false;
    }

    /*Launches the sort task in a thread*/
    @Override
    public String sortTask(String intermediateFilesFolder) throws TException {
        SortTaskHandler task = new SortTaskHandler(intermediateFilesFolder, server, loadProbability,timeTakenToSort);
        task.sortFiles();
        System.out.println("Total time taken for Sort tasks by this worker : " +timeTakenToSort);
        mapTaskStatistics.resetCountersMapJobCounters();
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
        MapTaskStatistics mapTaskStatistics = new MapTaskStatistics(mapTasksProcessd, timeTakenToMap);
        this.mapTaskStatistics = mapTaskStatistics;
        taskQueue = new ConcurrentLinkedQueue<>();
        this.loadProbability = loadProbability;
        WorkerTaskQueueHandler watcher = new WorkerTaskQueueHandler(this,taskQueue, server, self,this.mapTaskStatistics);
        watcher.start();
        // call enroll on superNode to enroll.
        protocol = serverClient.enroll(self);
        System.out.println("Worker node protocol received from server");

        serverTransport.close();

        WorkerStatistics display = new WorkerStatistics(mapTasksProcessd);
        display.start();
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
