package cost_aware_consistent_hashing;

import java.util.UUID;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode
public class Task {

    public Task(Long cost, UUID id){
        this.cost = cost;
        this.id = id;
    }

    //get time it took from when task was submitted until server was done processing
    //multiply by 1000 so it returns in microseconds
    public Long getElapsed(){
        return (finishTime-startTime)/1000;
    }

    //get time it took from when task was submitted until server picked it up
    //multiply by 1000 so it returns in microseconds
    public Long getQueuedTime(){
        return (dequeuedTime-startTime)/1000;
    }
    
    //amount of time to sleep for, for each task
    private Long cost;
    //used to identify which host to map the job to
    private UUID id;

    //timestamps per task so we can track queued time 
    private Long startTime;
    private Long dequeuedTime;
    private Long finishTime;
}
