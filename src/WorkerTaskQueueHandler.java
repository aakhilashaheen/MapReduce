import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class WorkerTaskQueueHandler extends Thread {
    private final ConcurrentLinkedQueue<String> requests; //synchronized
    private final WorkerHandler instance;
    Random rand;
    private static HashSet<String> positives,negatives;
    private Node server;
    private Node worker;
    private MapTaskStatistics mapTaskStatistics;

    public WorkerTaskQueueHandler(WorkerHandler instance, ConcurrentLinkedQueue<String> tasks, Node server, Node worker, MapTaskStatistics mapTaskStatistics) {
        this.requests = tasks;
        this.instance = instance;
        rand = new Random();
        this.server = server;
        this.worker = worker;
        this.mapTaskStatistics = mapTaskStatistics;
        //Initiate positive and negative files
        positives = new HashSet<>();
        try {
            FileReader fr = new FileReader("positive.txt");
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while((line = br.readLine()) != null) {
                positives.add(line.toLowerCase().replaceAll("\n", "").replaceAll("\r",""));
            }
            fr.close();
        } catch (Exception e) { e.printStackTrace(); }

        negatives = new HashSet<>();
        try {
            FileReader fr = new FileReader("negative.txt");
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while((line = br.readLine()) != null) {
                negatives.add(line.toLowerCase().replaceAll("\n", "").replaceAll("\r",""));
            }
            fr.close();
        } catch (Exception e) { e.printStackTrace(); }

    }

    @Override
    public void run() {
        while(true) {
            try {
                String task = null;
                while(requests.isEmpty()){
                    Thread.sleep(100);
                }

                //queue is no longer empty
                task = (String)requests.remove();
                if(task != null){
                  MapTaskHandler handler = new MapTaskHandler(task, positives, negatives, server, worker, instance.loadProbability, mapTaskStatistics);
                  handler.start();
                }
            } catch(Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }


}
