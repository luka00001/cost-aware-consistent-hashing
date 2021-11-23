package cost_aware_consistent_hashing;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import static cost_aware_consistent_hashing.Constants.*;

public class DataGeneratorTest {

    private DataGenerator dataGenerator = new DataGenerator();

    @Test
    public void testUniform() throws IOException{
        DataSet dataSet = dataGenerator.getDataset(DataSetType.UNIFORM);
        assertEquals(NUM_TASKS, dataSet.getTasks().size());
    }

    @Test
    public void testNormal() throws IOException{
        DataSet dataSet = dataGenerator.getDataset(DataSetType.NORMAL);
        assertEquals(NUM_TASKS, dataSet.getTasks().size()); 
    }

    @Test
    public void testCauchy() throws IOException{
        DataSet dataSet = dataGenerator.getDataset(DataSetType.CAUCHY);
        assertEquals(NUM_TASKS, dataSet.getTasks().size());
    }

    @Test
    public void testZipf() throws IOException{
        DataSet dataSet = dataGenerator.getDataset(DataSetType.ZIPF);
        assertEquals(NUM_TASKS, dataSet.getTasks().size());
    }

    @Test
    public void testConstant() throws IOException{
        DataSet dataSet = dataGenerator.getDataset(DataSetType.CONSTANT);
        assertEquals(NUM_TASKS, dataSet.getTasks().size());
    }

    @Test
    public void testCrypto() throws IOException{
        DataSet dataSet = dataGenerator.getDataset(DataSetType.CRYPTO);
        assertEquals(NUM_TASKS, dataSet.getTasks().size());
    }


    @Test
    public void testFacebook() throws IOException{
        DataSet dataSet = dataGenerator.getDataset(DataSetType.FACEBOOK);
        assertEquals(NUM_TASKS, dataSet.getTasks().size());
    }
}
