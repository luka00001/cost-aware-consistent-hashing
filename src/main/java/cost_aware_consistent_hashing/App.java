package cost_aware_consistent_hashing;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import static cost_aware_consistent_hashing.Constants.*;

/**
 * Currently unused but logic to run all the experiments and save the results could go here
 *
 */
public class App 
{
    private static final String[] HEADERS = {"dataSetType", "algorithm", "cacheEffectiveness", "totalTime",
        "maxLoadDiff", "maxJobsDiff", "costKurtosis", "costVariance", 
        "cost_p1", "cost_p10", "cost_p25", "cost_p50", "cost_p75", "cost_p90", "cost_p99", "cost_p100", 
        "latency_p1", "latency_p10", "latency_p25", "latency_p50", "latency_p75", "latency_p90", "latency_p99", "latency_p100", 
        "queued_p1", "queued_p10", "queued_p25", "queued_p50","queued_p75", "queued_p90", "queued_p99", "queued_p100" 
    };

    //how good is the cache for the experiment?
    private static final Double[] cacheEffectivessRates = {0D, .25, .75, 1D};

    public static void main( String[] args ) throws InterruptedException, IOException
    {
        DataGenerator dataGenerator = new DataGenerator();
        Controller controller = new Controller();
        List<ExperimentResults> results = new ArrayList<>();

        for(DataSetType dataSetType : DataSetType.values()) {
            for(int i = 0; i < NUM_EXPERIMENTS; i++){
                DataSet dataSet = dataGenerator.getDataset(dataSetType);
                for(Double cacheEffectiveness : cacheEffectivessRates){
                    for(AlgorithmType algorithmType : AlgorithmType.values()){
                        ExperimentResults result = controller.runExperiment(dataSet, algorithmType, cacheEffectiveness);
                        results.add(result);
                    }
                }
            }
       }
        writeCSV(results);
        System.exit(0);
    }

    /**
     * 
     * Write results to CSV, messy I know but for this just trying to get this done
     * @throws IOException
     */
    private static void writeCSV(List<ExperimentResults> results) throws IOException{
        Long currentTime = System.currentTimeMillis();
        String fileName = String.format("results_%s.csv", currentTime);
        FileWriter out = new FileWriter(fileName);
        try (CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(HEADERS))) {
            for(ExperimentResults result : results){
                Map<Integer, Double> cp = result.getCostsPercentiles();
                Map<Integer, Double> lp = result.getLatencyPercentiles();
                Map<Integer, Double> qp = result.getQueuedPercentiles();
                printer.printRecord(result.getDataSetType(), result.getAlgorithmType(), result.getCacheEffectivness(), result.getTotalTime(), 
                result.getMaxLoadDiff(), result.getMaxJobsDiff(), result.getCostKurtosis(), result.getCostVariance(),
                cp.get(1), cp.get(10), cp.get(25), cp.get(50), cp.get(75), cp.get(90), cp.get(99), cp.get(100),
                lp.get(1), lp.get(10), lp.get(25), lp.get(50), lp.get(75), lp.get(90), lp.get(99), lp.get(100),
                qp.get(1), qp.get(10), qp.get(25), qp.get(50), qp.get(75), qp.get(90), qp.get(99), qp.get(100) );
            }
        }
        System.out.println(String.format("Saved File %s", fileName));
    }
}
