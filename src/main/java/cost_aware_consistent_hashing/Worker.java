package cost_aware_consistent_hashing;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;

import lombok.Getter;

import static cost_aware_consistent_hashing.Constants.*;

@Getter
public class Worker implements Runnable {
    private BlockingQueue<Task> queue;
    private WorkerInfo workerInfo;
    private HashSet<UUID> cache = new HashSet<>();
    private final Double CACHE_EFFECTIVENESS;
    
    public Worker(BlockingQueue<Task> queue, WorkerInfo workerInfo, Double cacheEffectiveness) {
        this.queue = queue;
        this.workerInfo = workerInfo;
        this.CACHE_EFFECTIVENESS = cacheEffectiveness;
    }
    /**
     * Workers have a simple life cycle. While they are running repeate the following steps
     * 1. Try to take a task from the queue, if it is empty wait until their is a task
     * 2. If the task is the stope worker trigger, shut down
     * 3. Check if the task exists in the cache
     * 4. If it is not in the cache, sleep for the cost and then add it to the cache 
     * 5. If it is in the cahce, sleep for (1-CACHE_EFFECTIVENESS)*Cost. I.e. cache effectiveness of 1 reduces sleep time to 0
     * 
     */
    public void run() {
        try {
            while (true) {
                Task task = queue.take();
                task.setDequeuedTime(System.nanoTime());
                if (task.getCost().equals(STOP_WORKER)) {
                    return;
                }
                if(!cache.contains(task.getId())){
                    LockSupport.parkNanos(task.getCost()*1000);
                    //Thread.sleep(task.getCost());
                    cache.add(task.getId());
                }
                else{
                    Double cost = task.getCost()*(1-this.CACHE_EFFECTIVENESS);
                    //Thread.sleep(cost.longValue());
                    LockSupport.parkNanos(cost.longValue()*1000);
                }
                task.setFinishTime(System.nanoTime());
                workerInfo.addCompletedTask(task);
                workerInfo.incrementCount();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
