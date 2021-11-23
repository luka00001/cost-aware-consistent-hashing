package cost_aware_consistent_hashing;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
//Here a datasets is just a queue of tasks to be done
public class DataSet {
    DataSetType type;
    //Queue of tasks
    private List<Task> tasks = new ArrayList<>();
}
