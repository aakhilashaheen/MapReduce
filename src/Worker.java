import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Worker implements ComputeNodeService.Iface{

    Machine self;
    Machine server;
    double chanceToFail = 0.0;
    ConcurrentLinkedQueue<String> taskQueue;
    @Override
    public String mapTask(String inputFilename) throws TException {
        return null;
    }

    @Override
    public String sortTask(List<String> intermediateFilenames) throws TException {
        return null;
    }
    /* Constructor for a Server, a Thrift connection is made to the server as well */
    public Worker(String serverIP, Integer serverPort, Integer port, double chanceToFail) throws Exception {
        // connect to the server as a client
        TTransport serverTransport = new TSocket(serverIP, serverPort);
        serverTransport.open();
        TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(serverTransport));
        ServerService.Client serverClient = new ServerService.Client(serverProtocol);

        this.server = new Machine();
        this.server.ipAddress = serverIP;
        this.server.port = serverPort;

        //Create a Machine data type representing ourselves
        self = new Machine();
        self.ipAddress = InetAddress.getLocalHost().getHostName().toString();
        self.port = port;


        this.chanceToFail = chanceToFail;

        // call enroll on superNode to enroll.
        int protocol = serverClient.enroll(self);
        System.out.println("Worker node protocol received from server");
        taskQueue = new ConcurrentLinkedQueue<>();



        serverTransport.close();
    }
    //Begin Thrift Server instance for a Node and listen for connections on our port
    private void start() throws TException {

        //Create Thrift server socket
        TServerTransport serverTransport = new TServerSocket(self.port);
        TTransportFactory factory = new TFramedTransport.Factory();

        ComputeNodeService.Processor processor = new ComputeNodeService.Processor<>(this);

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
            System.err.println("Usage: java ComputeNodeHandler <serverIP> <serverPort> <port>");
            return;
        }
        try {
            System.out.println("IP Address is " + InetAddress.getLocalHost().toString());
            String serverIP = args[0];
            Integer serverPort = Integer.parseInt(args[1]);
            Double temp = new Double(args[3]);
            double chanceToFail = temp.doubleValue();
            //port number used by this node.
            Integer port = Integer.parseInt(args[2]);

            Worker server = new Worker(serverIP, serverPort,port, chanceToFail);

            //spin up server
            server.start();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
