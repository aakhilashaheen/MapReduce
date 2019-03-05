import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TaskQueueWatcher extends Thread {
    private final ConcurrentLinkedQueue<String> requests; //synchronized
    private final Worker instance;
    Random rand;

    public TaskQueueWatcher(Worker instance,ConcurrentLinkedQueue<String> tasks) {
        this.requests = tasks;
        this.instance = instance;
        rand = new Random();
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
//                    SortMerge handler = new SortMerge(task);
//                    handler.start();
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
