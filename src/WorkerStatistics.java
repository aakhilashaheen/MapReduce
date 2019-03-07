import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
/* Global worker statistics counter
 */
public class WorkerStatistics extends Thread {

    private static AtomicInteger mapTasksCompleted;
    private static AtomicInteger mapTasksReceived;

    public WorkerStatistics(AtomicInteger mapTasksCompleted, AtomicInteger mapTasksReceived) {
        this.mapTasksCompleted = mapTasksCompleted;
        this.mapTasksReceived = mapTasksReceived;
    }

    public void run() {
        long startTime = System.currentTimeMillis();
        while (!mapTasksReceived.equals(mapTasksCompleted)) {
            long completed = mapTasksCompleted.longValue();
            if (completed == 0) {
                System.out.println("Average task completion time currently: " +
                        (System.currentTimeMillis() - startTime));
            } else {
                System.out.println("Average task completion time currently: " +
                        ((System.currentTimeMillis() - startTime) / (completed)));

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
