import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MapTaskSender implements Runnable {

    private List<Machine> nodes;
    private ConcurrentLinkedQueue<String> inputFiles;

    public MapTaskSender(List<Machine> computeNodes, ConcurrentLinkedQueue<String> inputFiles){
        this.nodes = computeNodes;
        this.inputFiles = inputFiles;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+" Start ");
        sendRpcCall();
        System.out.println(Thread.currentThread().getName()+" End.");
    }

    private void sendRpcCall() {
        try {
            Random rand = new Random();
            int index = rand.nextInt(nodes.size());
            Machine m = nodes.get(index);
            TTransport computeTransport = new TSocket(m.ipAddress, m.port);
            computeTransport.open();
            TProtocol computeProtocol = new TBinaryProtocol(new TFramedTransport(computeTransport));
            ComputeNodeService.Client computeNode  = new ComputeNodeService.Client(computeProtocol);
            if(!inputFiles.isEmpty()){
                String inputFile = inputFiles.remove();
               String outputFile =  computeNode.mapTask(inputFile);
               if(outputFile == null){
                   inputFiles.add(inputFile);
               }
            }

            computeTransport.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
