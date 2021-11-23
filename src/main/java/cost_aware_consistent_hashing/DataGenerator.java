package cost_aware_consistent_hashing;

import static cost_aware_consistent_hashing.Constants.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.distribution.CauchyDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;

public class DataGenerator {
    private Random random = new Random();
    
    //Generate a Dataset type from one of the known datasettypes
    public DataSet getDataset(DataSetType dataSetType) throws IOException{
        DataSet dataset;

        switch(dataSetType){
            case CONSTANT: dataset = constantDataSet(); break;
            case NORMAL: dataset = normalDataSet(); break;
            case UNIFORM: dataset = uniformDataSet(); break;
            case CAUCHY: dataset = caucyDataSet(); break;
            case ZIPF: dataset = zipfDataSet(); break;
            case CRYPTO: dataset = cryptoDataSet(); break;
            case FACEBOOK: dataset = facebookDataSet(); break;
            default: throw new RuntimeException(String.format("Dataset Type %s, did not match any configured type", dataSetType));
        }
        dataset.setType(dataSetType);

        return dataset;
    }

    //genreate normal distribution by sampling from a normal distribution 
    private DataSet normalDataSet(){
        //create normal distribution with mean .05 and sd .05/3 so that 99.7% of events are < .1
        NormalDistribution normalDistribution = new NormalDistribution(TARGET_MEAN, TARGET_MEAN/3D);
        DataSet dataSet = new DataSet();
        Task[] tasks = new Task[NUM_DISTINCT_TASKS];
        //generate costs from sampling from normal distribution
        for(int i=0; i < NUM_DISTINCT_TASKS; i++){
            UUID uuid = UUID.randomUUID();
            Long cost = (long) Math.max(1, normalDistribution.sample()); //make sure no 0 cost events
            tasks[i] = new Task(cost, uuid);
        }
        //generate task multiplities from sampling from uniform distribution
        for(int i=0; i < NUM_TASKS; i++){
            dataSet.getTasks().add(tasks[random.nextInt(NUM_DISTINCT_TASKS)]);
        }
        return dataSet;
    }

    private DataSet caucyDataSet(){
        //See https://keisan.casio.com/exec/system/1180573167 for what percentiles of this distribution look like
        CauchyDistribution cauchyDistribution = new CauchyDistribution(TARGET_MEAN, TARGET_MEAN/10);
        DataSet dataSet = new DataSet();
        Task[] tasks = new Task[NUM_DISTINCT_TASKS];
        //generate costs from sampling from normal distribution
        for(int i=0; i < NUM_DISTINCT_TASKS; i++){
            UUID uuid = UUID.randomUUID();
            Long cost = (long) Math.max(1, cauchyDistribution.sample()); //make sure no 0 cost events
            tasks[i] = new Task(cost, uuid);
        }
        //generate task multiplities from sampling from uniform distribution
        for(int i=0; i < NUM_TASKS; i++){
            dataSet.getTasks().add(tasks[random.nextInt(NUM_DISTINCT_TASKS)]);
        }
        return dataSet;
    }

    private DataSet zipfDataSet(){
        //this produces a mean of close to 5000 which is what the others have
        ZipfDistribution zipfDistribution = new ZipfDistribution(NUM_DISTINCT_TASKS, 1);
        //Magic number which is computed the following way
        //Take the H_{2500,1}/(1+4+9+...) * Target Mean
        //this will result in mean of 50
        Double multiplier = (8.4/1.644)*TARGET_MEAN;
        DataSet dataSet = new DataSet();
        Map<Integer, UUID> ids = new HashMap<>();
        //generate costs from sampling from normal distribution
        for(int i=0; i < NUM_TASKS; i++){
            //zipf gives numbers from 1 to NUM_DISTINCT_TASKS where 1 is most frequent
            int sample = zipfDistribution.sample();
            UUID id = ids.getOrDefault(sample, UUID.randomUUID());
            ids.put(sample, id);
            Double cost = multiplier * (1D/sample);
            dataSet.getTasks().add(new Task(cost.longValue(), id));
        }
        return dataSet;
    }

    private DataSet uniformDataSet(){
        DataSet dataSet = new DataSet();
        Task[] tasks = new Task[NUM_DISTINCT_TASKS];
        //generate costs from sampling from unifrom distribution over (0,.1)
        for(int i=0; i < NUM_DISTINCT_TASKS; i++){
            UUID uuid = UUID.randomUUID();
            Long upperBound = TARGET_MEAN*2+1;
            Long cost = (long) random.nextInt(upperBound.intValue());
            tasks[i] = new Task(cost, uuid);
        }
        //generate task multiplities from sampling from uniform distribution
        for(int i=0; i < NUM_TASKS; i++){
            dataSet.getTasks().add(tasks[random.nextInt(NUM_DISTINCT_TASKS)]);
        }
        return dataSet;
    }

    /**
     * Dataset with all 50 mills time, i.e. dataset like what was discussed in the paper
     */
    private DataSet constantDataSet(){
        DataSet dataSet = new DataSet();
        Task[] tasks = new Task[NUM_DISTINCT_TASKS];
        //generate taks with all 50 mills
        for(int i=0; i < NUM_DISTINCT_TASKS; i++){
            UUID uuid = UUID.randomUUID();
            tasks[i] = new Task(TARGET_MEAN, uuid);
        }
        for(int i=0; i < NUM_TASKS; i++){
            dataSet.getTasks().add(tasks[random.nextInt(NUM_DISTINCT_TASKS)]);
        }
        return dataSet;
    }

    /**
     * Get crypto dataset
     * @throws IOException
     */
    private DataSet cryptoDataSet() throws IOException{
        DataSet dataSet = new DataSet();
        String[] headers = {"symbol", "shares_traded"};
        Reader in = new FileReader("src/main/resources/crypto.csv");
        Map<String, UUID> map = new HashMap<>();
        Iterable<CSVRecord> records = CSVFormat.DEFAULT
          .withHeader(headers)
          .withFirstRecordAsHeader()
          .parse(in);
        List<Double> costs = new ArrayList<>();
        List<String> symbols = new ArrayList<>();
        for (CSVRecord record : records) {
            Double cost = Double.parseDouble(record.get("shares_traded"));
            String symbol = record.get("symbol");
            if(cost == 0D){
                continue;
            }
            costs.add(cost);
            symbols.add(symbol);
            map.put(symbol, UUID.randomUUID());
        }
        Double mean = costs.stream().mapToDouble(x->x).average().getAsDouble();
        Double multiplier = TARGET_MEAN/mean;
        for(int i=0; i < NUM_TASKS; i++){
            int randInt = random.nextInt(costs.size());            
            Double cost = costs.get(randInt)*multiplier;
            String symbol = symbols.get(randInt);
            Task task = new Task(cost.longValue(), map.get(symbol));
            dataSet.getTasks().add(task);
        }
        return dataSet;
    }

    private DataSet facebookDataSet() throws FileNotFoundException{
        DataSet dataSet = new DataSet();
        File myObj = new File("src/main/resources/facebook.txt");
        Scanner myReader = new Scanner(myObj);
        Map<String, Long> counts = new HashMap<>();
        Map<String, UUID> ids = new HashMap<>();
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            String[] splits = data.split(" ");
            String id = splits[1];
            Long currentCount = counts.containsKey(id) ? counts.get(id) : 0L;
            counts.put(id, currentCount+1);
            if(!ids.containsKey(id)){
                ids.put(id, UUID.randomUUID());
            }
        }
        myReader.close();

        String[] edges = ids.keySet().toArray(new String[0]);
        Double mean = counts.values().stream().mapToDouble(x->x).average().getAsDouble();
        Double multiplier = TARGET_MEAN/mean;
        for(int i=0; i < NUM_TASKS; i++){
            String randomEdge = edges[random.nextInt(counts.size())];            
            Double cost = counts.get(randomEdge)*multiplier;
            Task task = new Task(cost.longValue(), ids.get(randomEdge));
            dataSet.getTasks().add(task);
        }
        return dataSet;     
    }
}
