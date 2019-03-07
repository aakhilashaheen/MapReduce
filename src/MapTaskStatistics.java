import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MapTaskStatistics {
    private  AtomicInteger numberOfMapTasksProcessed = new AtomicInteger(0);
    private  AtomicLong timeTakenToExecuteMapTasks = new AtomicLong(0);


    public int getNumberOfMapTaskssProcessed(){
        return numberOfMapTasksProcessed.intValue();
    }

    public long getTimeTakenToExecuteMapTasks(){
        return timeTakenToExecuteMapTasks.longValue();
    }

    public  int incrementAndGetCounts(){
        return numberOfMapTasksProcessed.getAndIncrement();
    }

    public  long incrementAndGetTimeTakenForMapTasks(long timeTaken){
        System.out.println("Adding time"+timeTaken);
        return timeTakenToExecuteMapTasks.getAndAdd(timeTaken);
    }

    public void resetCountersMapJobCounters(){
        numberOfMapTasksProcessed.set(0);
        timeTakenToExecuteMapTasks.set(0l);
    }
}
