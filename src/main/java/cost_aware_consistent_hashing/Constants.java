package cost_aware_consistent_hashing;

public final class Constants {
    
    private Constants() {
        // restrict instantiation
    }

    //how many threads to use
    public static final int NUM_SERVERS = 10;
    //For contrived datasets, how many tasks are there
    public static final int NUM_TASKS = 10000;
    //For contrived datasets, how many of the tasks are distinct (we will then randomally generate repeats)
    public static final int NUM_DISTINCT_TASKS = 2500;
    //Used to tell the workers to stop draining the queues at the end of the experiment
    public static final Long STOP_WORKER = -1L;
    //how many tasks to publish at once
    public static final int BATCH_SIZE = 100;
    //Epsilon, as in the parameter descibed in the paper Consistent Hasing with Bounded Loads
    public static final Double EPSILON = .3;
    //In consistent hasing we generally dont just map the servers in a circle but instead insert duplicates as well
    //this improves even load and scaling behaivor 
    public static final int NUM_REPLICAS = 5;
    //number of times to run each experiment
    public static final int NUM_EXPERIMENTS = 5; 
    //goal is to have datasets have a mean of 5000 Microseconds/5 milliseconds
    public static final Long TARGET_MEAN = 5000L;
    //how long to wait after publishing a batch until the next one is submitted
    public static final Long BATCH_TIME = TARGET_MEAN*NUM_SERVERS; 
    //if something has more than (1+ELAPSED_EPLISION) average load skip
    public static final Double ELAPSED_EPLISION = 10D;
    //How long to remember tasks for, right now set to the time for two batches
    public static final Long MEMORY_TIME = BATCH_TIME*2;
    //Number of choices to use in multiple choice based algorithms
    public static final int NUM_CHOICES = 3;
}
