package cost_aware_consistent_hashing;

import static cost_aware_consistent_hashing.Constants.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class ServerDecider {
    //use the MD5 hash function for consistent hashing as it "mixes" well
    final HashFunction hashFunction = Hashing.hmacMd5("myKey".getBytes());
    private final TreeMap<String, Integer> map = new TreeMap<>();
    private final TreeMap<String, Integer> singularMap = new TreeMap<>();
    private List<HashFunction> rehashFunctions = new ArrayList<>();
    private Random random = new Random();

    //Initialize map with replicas 
    public ServerDecider(){
        //map servers into the treemap so that we can use them in consistent hashing
        for(int i = 0; i < NUM_SERVERS; i++){
            for(int j = 0; j < NUM_REPLICAS; j++){
                map.put(hashFunction.hashInt(random.nextInt()).toString(), i);
            }
        }
        //map servers into the treemap one time for consistent singular
        for(int i = 0; i < NUM_SERVERS; i++){
            singularMap.put(hashFunction.hashInt(random.nextInt()).toString(), i);
        }
        //add rehash functions for our rehash function strategy 
        for(Integer i = 0; i < NUM_SERVERS; i++){
            rehashFunctions.add(Hashing.hmacMd5(i.toString().getBytes()));
        }
    } 

    public int hash(AlgorithmType algorithmType, Task task, WorkerInfo[] workerInfos, BlockingQueue<Task>[] queues){
        int out;
        switch(algorithmType){
            case MODULO : out = moduloHash(task); break;
            case CONSISTENT : out = consistentHash(task); break;
            case CONSISTENT_SINGULAR : out = consistentSingularHash(task); break;
            case BOUNDED_LOAD : out = boundedLoad(task, queues); break;
            case REHASH : out = rehash(task, queues); break;
            case BOUNDED_ELAPSED : out = boundedElapsed(task, workerInfos, queues); break;
            case MIN_CHOICE: out = minChoice(task, queues); break;
            case MIN_CHOICE_ELAPSED: out = minChoiceElapsed(task, workerInfos); break;
            default : throw new RuntimeException(String.format("Algorithm Type %s, did not match any configured type", algorithmType));
        }
        return out;
    }
    
    //Basic Algos 101 modulus hash
    private int moduloHash(Task task){
        return Math.abs(task.getId().hashCode()) % NUM_SERVERS;
    }

    /*
    Use a tree map to get a sorted order of the servers (imagine the servers being circle)
    Map the task to a point on the cirlce and then find the server that is closet, but larger to 
    the hash. The key thing here is we are using the same hash function to map both the servers
    and the tasks to the cirlce. 
    */
    private int consistentHash(Task task){
        String hash = hashFunction.hashBytes(task.getId().toString().getBytes()).toString();
        if(!map.containsKey(hash)){
            SortedMap<String, Integer> tailMap = map.tailMap(hash);
            hash = tailMap.isEmpty() ? map.firstKey() : tailMap.firstKey();
        }
        return map.get(hash);
    }

    /**
     * 
     * Conistent hashing but with only one bucket per server
     */
    private int consistentSingularHash(Task task){
        String hash = hashFunction.hashBytes(task.getId().toString().getBytes()).toString();
        if(!singularMap.containsKey(hash)){
            SortedMap<String, Integer> tailMap = singularMap.tailMap(hash);
            hash = tailMap.isEmpty() ? singularMap.firstKey() : tailMap.firstKey();
        }
        return singularMap.get(hash);
    }

    /*
    * Implement Consistent Hashing with Bounded Load
    * Use constant Epsilon, to make sure that load per server is less than (1+eplsilon) the average load
    */
    private int boundedLoad(Task task, BlockingQueue<Task>[] queues){
        String hash = hashFunction.hashBytes(task.getId().toString().getBytes()).toString();
        SortedMap<String, Integer> tailMap = map.tailMap(hash);
        for(Map.Entry<String, Integer> entry : tailMap.entrySet()){
            int server = entry.getValue();
            if(queues[server].size() <= (1+EPSILON)*((double)BATCH_SIZE/NUM_SERVERS)){
                return server;
            }
        }
        for(Map.Entry<String, Integer> entry : map.entrySet()){
            int server = entry.getValue();
            if(queues[server].size() <= (1+EPSILON)*((double)BATCH_SIZE/NUM_SERVERS)){
                return server;
            }
        }
        hash = tailMap.isEmpty() ? map.firstKey() : tailMap.firstKey();
        return map.get(hash);
    }

    /*
    * Like bounded loads however instead of going around the cirlce to find next element
    * rehash to a random element and then check again 
    */
    private int rehash(Task task, BlockingQueue<Task>[] queues){
        for(int i=0; i < NUM_SERVERS; i++){
            String hash = rehashFunctions.get(i).hashBytes(task.getId().toString().getBytes()).toString();
            Entry<String, Integer> entry = map.ceilingEntry(hash);
            int server = entry == null ? map.floorEntry(hash).getValue() : entry.getValue();
            if(queues[server].size() <= (1+EPSILON)*((double)BATCH_SIZE/NUM_SERVERS)){
                return server;
            }
        }
        return random.nextInt(NUM_SERVERS); //default to random server if everything is full
    }

    /*
    * Implement Consistent Hashing with Bounded Load, but bounded load on average elapsed instead of num jobs
    * Use constant Epsilon, to make sure that load per server is less than (1+eplsilon) the average load
    * in terms of elapsed
    */
    private int boundedElapsed(Task task, WorkerInfo[] workerInfos, BlockingQueue<Task>[] queues){
        Double avgJobPerSev = ((double) BATCH_SIZE)/NUM_SERVERS;
        Double allowedElapsed = (1+ELAPSED_EPLISION)*((double)TARGET_MEAN)*avgJobPerSev/2;
        for(int i=0; i < NUM_SERVERS; i++){
            String hash = rehashFunctions.get(i).hashBytes(task.getId().toString().getBytes()).toString();
            Entry<String, Integer> entry = map.ceilingEntry(hash);
            int server = entry == null ? map.floorEntry(hash).getValue() : entry.getValue();
            if(queues[server].isEmpty() || workerInfos[server].getAverageElapsed() <= allowedElapsed){
                return server;
            }
        }
        return random.nextInt(NUM_SERVERS); //default to random server if everything is full
    }

     //min queue size of NUM_CHOICE 
     private int minChoice(Task task, BlockingQueue<Task>[] queues){
        int minServer = 0;
        int minQueueSize = Integer.MAX_VALUE;
        for(int i=0; i < NUM_CHOICES; i++){
            String hash = rehashFunctions.get(i).hashBytes(task.getId().toString().getBytes()).toString();
            Entry<String, Integer> entry = map.ceilingEntry(hash);
            int server = entry == null ? map.floorEntry(hash).getValue() : entry.getValue();
            int queueSize = queues[server].size();
            if(queueSize == 0){
                return server;
            }
            if(queueSize < minQueueSize){
                minServer = server;
                minQueueSize = queueSize;
            }
        }
        return minServer;
    }

     //min avg elapased over  of NUM_CHOICE 
     private int minChoiceElapsed(Task task, WorkerInfo[] workerInfos){
        int minServer = 0;
        Double minAverage = Double.MAX_VALUE;
        for(int i=0; i < NUM_CHOICES; i++){
            String hash = rehashFunctions.get(i).hashBytes(task.getId().toString().getBytes()).toString();
            Entry<String, Integer> entry = map.ceilingEntry(hash);
            int server = entry == null ? map.floorEntry(hash).getValue() : entry.getValue();
            Double average = workerInfos[server].getAverageElapsed();
            if(average == 0D){
                return server;
            }
            if(average < minAverage){
                minServer = server;
                minAverage = average;
            }
        }
        return minServer;
    }
}
