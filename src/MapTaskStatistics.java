import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
/*
Statistics class for map jobs at the worker
 */
public class MapTaskStatistics {
    private  AtomicInteger numberOfMapTasksProcessed;
    private  AtomicLong timeTakenToExecuteMapTasks;

    public MapTaskStatistics(AtomicInteger numberOfMapTasksProcessed, AtomicLong timeTakenToExecuteMapTasks) {
        this.numberOfMapTasksProcessed = numberOfMapTasksProcessed;
        this.timeTakenToExecuteMapTasks = timeTakenToExecuteMapTasks;
    }

    public int getNumberOfMapTaskssProcessed(){
        return numberOfMapTasksProcessed.intValue();
    }

    public long getTimeTakenToExecuteMapTasks(){
        return timeTakenToExecuteMapTasks.longValue();
    }

    public  int incrementAndGetCounts(){
        numberOfMapTasksProcessed.getAndIncrement();
        return numberOfMapTasksProcessed.intValue();
    }

    public  long incrementAndGetTimeTakenForMapTasks(long timeTaken){
        System.out.println("Adding time"+timeTaken);
        timeTakenToExecuteMapTasks.getAndAdd(timeTaken);
        return timeTakenToExecuteMapTasks.longValue();
    }

    public void resetCountersMapJobCounters(){
        numberOfMapTasksProcessed.set(0);
        timeTakenToExecuteMapTasks.set(0l);
    }
}
