import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.Random;

public class MapTaskSender implements Runnable {

    private List<Node> nodes;

    private String inputFile;

    public MapTaskSender(List<Node> computeNodes, String inputFile){
        this.nodes = computeNodes;
        this.inputFile = inputFile;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+" Start ");
        sendRpcCall();
        System.out.println(Thread.currentThread().getName()+" End.");
    }

    private void sendRpcCall() {
        boolean inputFileBeingProcessed = false;
            while(!inputFileBeingProcessed) {
                Random rand = new Random();
                int index = rand.nextInt(nodes.size());
                Node m = nodes.get(index);
                try{
                    TTransport computeTransport = new TSocket(m.ipAddress, m.port);
                    computeTransport.open();
                    TProtocol computeProtocol = new TBinaryProtocol(new TFramedTransport(computeTransport));
                    WorkerNodeService.Client workerNode  = new WorkerNodeService.Client(computeProtocol);
                    inputFileBeingProcessed = workerNode.mapTask(inputFile);
                    computeTransport.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
    }

}
