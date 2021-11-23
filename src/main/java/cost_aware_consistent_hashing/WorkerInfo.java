package cost_aware_consistent_hashing;

import java.util.ArrayDeque;

import lombok.Getter;
import lombok.Setter;

import static cost_aware_consistent_hashing.Constants.*;

/**
 * WorkerInfo - user for sending data from the worker to the controller
 * so that the controller can make informed decisions on where to route
 * 
 */
@Getter
@Setter
public class WorkerInfo {
    private int numJobs = 0;
    private double totalElapsed = 0;
    private ArrayDeque<Task> taskQueue = new ArrayDeque<>();

    public void incrementCount(){
        numJobs++;
    }

    public void addCompletedTask(Task task){
        totalElapsed += task.getElapsed();
        taskQueue.add(task);
    }

    //get average elapsed time of tasks that in the last MEMORY_TIME seconds
    public synchronized Double getAverageElapsed(){
        //remove tasks older than MEMORY_TIME
        while(!taskQueue.isEmpty() && (System.nanoTime() - taskQueue.getFirst().getFinishTime()) > MEMORY_TIME*1000){
            totalElapsed -= taskQueue.removeFirst().getElapsed();
        }
        return taskQueue.isEmpty() ? 0 : totalElapsed/taskQueue.size();
    }
}
