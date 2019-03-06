import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.time.Instant;
import java.util.HashSet;

public class SortTaskSender extends Thread {

    private String intermediateDirectory;
    private Node workerNode;

    public SortTaskSender(Node n, String intermediateDirectory){
        this.workerNode = n;
        this.intermediateDirectory = intermediateDirectory;
    }
    @Override
    public void run() {
        sendSortTask();

    }

    public void sendSortTask(){
        try{
            TTransport computeTransport = new TSocket(workerNode.ipAddress, workerNode.port);
            computeTransport.open();
            TProtocol computeProtocol = new TBinaryProtocol(new TFramedTransport(computeTransport));
            WorkerNodeService.Client computeNode  = new WorkerNodeService.Client(computeProtocol);
            computeNode.sortTask(intermediateDirectory);
            computeTransport.close();
        }catch(Exception e){
            System.out.println("The sort job failed" + e);
        }
    }


}