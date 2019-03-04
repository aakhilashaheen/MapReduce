import org.apache.thrift.TException;

import java.util.List;

public class ServerHandler implements ServerService.Iface{
    private static int serverPort;
    private static int schedulingPolicy;

    @Override
    public int enroll(Machine machine) throws TException {
        return 0;
    }

    @Override
    public String mapReduceJob(List<String> inputfileNames) throws TException {
        System.out.println("Received list to mapreduce");
        while()
        return null;
    }

    public static void main(String [] args){
        // Inputs should be the port of the server, scheduling policy as 0/1
        if(args.length < 2){
            System.out.println("Please enter the port and the scheduling policy");
            return;
        }

        this.serverPort = Integer.parseInt(args[0]);
        this.schedulingPolicy = Integer.parseInt(args[1]);

       //Thrift code for starting the server handler
    }

}
