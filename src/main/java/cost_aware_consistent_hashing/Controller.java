package cost_aware_consistent_hashing;

import lombok.Getter;
import static cost_aware_consistent_hashing.Constants.*;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

@Getter
public class Controller {
    DataGenerator dataGenerator = new DataGenerator();
    private int maxLoadDiff;
    
    /*
    For a given dataset and algorithm run the experiment
    This means take the dataset, in batches publish it to the worker threads,
    have the threads sleep for the cost of each task
    and then publish time it took to run the experiment
    */
    public ExperimentResults runExperiment(DataSet dataSet, AlgorithmType algorithmType,
        Double cacheEffectivness) throws InterruptedException{
        System.out.println(String.format("Starting Experiment with Dataset Type %s, Algorithm Type %s", dataSet.getType(), algorithmType));
        ExperimentResults results = new ExperimentResults();
        ServerDecider serverDecider = new ServerDecider();
        maxLoadDiff = Integer.MIN_VALUE;

        WorkerInfo[] workerInfos = new WorkerInfo[NUM_SERVERS];
        BlockingQueue<Task>[] queues = new ArrayBlockingQueue[NUM_SERVERS];

        //Start the worker threads
        for(int i=0; i < NUM_SERVERS; i++){
            workerInfos[i] = new WorkerInfo();
            queues[i] = new ArrayBlockingQueue<>(NUM_TASKS);
            new Thread(new Worker(queues[i], workerInfos[i], cacheEffectivness)).start();
        }

        final long startTime = System.currentTimeMillis();

        int numBatches = NUM_TASKS/BATCH_SIZE;
        for(int i=0; i < numBatches; i++){
            System.out.println(String.format("Working on Batch %d of %d", i+1, numBatches));
            if((i+1) % 10 == 0){
                for(WorkerInfo workerInfo : workerInfos){
                    System.out.println(workerInfo.getAverageElapsed());
                }
            }
            //publish batch of tasks to appropriate queues
            for(int j = 0; j < BATCH_SIZE; j++){
                Task task = dataSet.getTasks().get(BATCH_SIZE*i + j);
                int serverNum = serverDecider.hash(algorithmType, task, workerInfos, queues);
                task.setStartTime(System.nanoTime());
                queues[serverNum].add(task);
            }
            //wait for all queues to be cleared
            long batchStart = System.nanoTime();
            while(true){
                //if no workers have tasks yet, or if the batch has taken longer than BATCH_TIME
                //move on and start publihsing the tasks of the next batch
                if(System.nanoTime() - batchStart > BATCH_TIME*1000){
                    break;
                }
                if(!pendingTasks(queues) ){
                    break;
                }
               // Thread.sleep(5L); //backoff 
            }
        }
        //make sure all the tasks are drained before ending the expierment
        while(true){
            if(!pendingTasks(queues)){
                break;
            }
          //  Thread.sleep(1L); //backoff 
        }
        final long endTime = System.currentTimeMillis();

        //Make objects so that we can get easy summary statistics
        DescriptiveStatistics costsStats = new DescriptiveStatistics(dataSet.getTasks().stream().map(Task::getCost).mapToDouble(d -> d).toArray());
        DescriptiveStatistics queuedStats = new DescriptiveStatistics(dataSet.getTasks().stream().map(Task::getQueuedTime).mapToDouble(d -> d).toArray());

        DescriptiveStatistics elapsedStats;
        try{
            elapsedStats = new DescriptiveStatistics(dataSet.getTasks().stream().map(Task::getElapsed).mapToDouble(d -> d).toArray());
        }
        catch(NullPointerException e){
            Thread.sleep(2000L); //Sleep for 2 seconds in case a task is still sleeping on a worker
            elapsedStats = new DescriptiveStatistics(dataSet.getTasks().stream().map(Task::getElapsed).mapToDouble(d -> d).toArray());
        }
        int maxJobs = Integer.MIN_VALUE;
        int minJobs = Integer.MAX_VALUE;
        for(WorkerInfo workerInfo : workerInfos){
            int numJobs = workerInfo.getNumJobs();
            minJobs = Math.min(minJobs, numJobs);
            maxJobs = Math.max(maxJobs, numJobs);
            System.out.println(String.format("Num Jobs: %s", numJobs));
        }
   
        //add in summary statistics to the results
        results.setDataSetType(dataSet.getType());
        results.setAlgorithmType(algorithmType);
        results.setCacheEffectivness(cacheEffectivness);
        results.setTotalTime(endTime-startTime);
        results.setCostVariance(costsStats.getVariance());
        results.setCostKurtosis(costsStats.getKurtosis());
        results.setCostsPercentiles(percentiles(costsStats));
        results.setLatencyPercentiles(percentiles(elapsedStats));
        results.setQueuedPercentiles(percentiles(queuedStats));
        results.setMaxLoadDiff(maxLoadDiff);
        results.setMaxJobsDiff(maxJobs-minJobs);

        System.out.println(String.format("Experiment with Dataset Type %s, Algorithm Type %s, took %d", dataSet.getType(), algorithmType, endTime-startTime));
        System.out.println(results.toString());

        //Stop worker threads
        for(int i=0; i < NUM_SERVERS; i++){
            queues[i].add(new Task(STOP_WORKER, UUID.randomUUID()));
        }
        return results;
    }

    //Check if all of the workers have drained all their tasks
    private boolean pendingTasks(BlockingQueue<Task>[] queues){
        boolean pendingTasks = false;
        int minRequests = Integer.MAX_VALUE;
        int maxRequests = Integer.MIN_VALUE;
        for(BlockingQueue<Task> queue : queues){
            int size = queue.size();
            minRequests = Math.min(size, minRequests);
            maxRequests = Math.max(size, maxRequests);
            if(size != 0){
                pendingTasks = true;
            }
        }
        maxLoadDiff = Math.max(maxLoadDiff, maxRequests-minRequests);
        return pendingTasks;
    }

    private Map<Integer, Double> percentiles(DescriptiveStatistics desc){
        return Map.of(
            1,   desc.getPercentile(1),
            10,  desc.getPercentile(10),
            25,  desc.getPercentile(25),
            50,  desc.getPercentile(50),
            75,  desc.getPercentile(75),
            90,  desc.getPercentile(90),
            99,  desc.getPercentile(99),
            100, desc.getPercentile(100)
        );
    }
}
