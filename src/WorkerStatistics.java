import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WorkerStatistics extends Thread {

    private static AtomicInteger mapTasksCompleted;

    public WorkerStatistics(AtomicInteger mapTasksCompleted) {
        this.mapTasksCompleted = mapTasksCompleted;
    }

    public void run() {
        long startTime = System.currentTimeMillis();
        while(true) {
            long completed = mapTasksCompleted.longValue();
            if(completed == 0) {
                System.out.println("Average task completion time: " +
                        Long.toString(System.currentTimeMillis() - startTime));
            }
            else {
                System.out.println("Average task completion time: " +
                        Long.toString((System.currentTimeMillis() - startTime)/(completed)));
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
