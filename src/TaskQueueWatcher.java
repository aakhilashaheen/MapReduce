import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TaskQueueWatcher extends Thread {
    private final ConcurrentLinkedQueue<String> requests; //synchronized
    private final Worker instance;
    Random rand;
    private static HashSet<String> positives,negatives;

    public TaskQueueWatcher(Worker instance,ConcurrentLinkedQueue<String> tasks) {
        this.requests = tasks;
        this.instance = instance;
        rand = new Random();

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
                task = requests.remove();

                if(task != null){
                    injectDelay();
                  MapTask handler = new MapTask(task,positives,negatives);
                  handler.start();
                }
            } catch(Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    private void injectDelay(){
        double roll = rand.nextDouble();
        if(roll < instance.loadProbability){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
